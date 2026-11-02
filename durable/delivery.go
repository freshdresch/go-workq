package durable

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
)

type claim struct {
	used     bool
	acquired bool
	delivery Delivery
	charge   Bytes
}

// StopDelivery prevents later acquisition and preserves attempts already acquired.
// The coordinator reclaims unpublished work after its current storage read ends.
func (q *Queue) StopDelivery() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.stopDeliveryLocked()
}

func (q *Queue) stopDeliveryLocked() {
	q.deliveryStopped = true
	q.signalLocked()
}

func (q *Queue) closeDeliveryLocked() {
	if q.deliveryClosed {
		return
	}
	// Only the coordinator calls this, after its last dispatch operation.
	q.deliveryQueue.Close()
	q.deliveryClosed = true
	for {
		count, _ := q.deliveryQueue.TryReceive(q.receiveSlot[:])
		if count == 0 {
			break
		}
		q.clearClaimLocked(q.receiveSlot[0])
	}
	q.signalLocked()
}

func (q *Queue) clearClaimLocked(index int) {
	slot := &q.claims[index]
	if !slot.used {
		return
	}
	q.deliveryRetained -= slot.charge
	*slot = claim{}
	q.cacheStateLocked()
}

func (q *Queue) claimLocked(id ID) int {
	for index := range q.claims {
		if q.claims[index].used && q.claims[index].delivery.ID == id {
			return index
		}
	}
	return -1
}

// Receive acquires up to limit available attempts without waiting to fill the batch.
// Once acquisition wins, it returns deliveries and nil even if cancellation or
// StopDelivery follows. An error always returns nil deliveries.
func (q *Queue) Receive(ctx context.Context, limit int) ([]Delivery, error) {
	return q.receiveObserved(ctx, limit, nil)
}

type receiveObserver interface{ Acquired() }

type deliveryReadObserver interface{ BeforeDeliveryRead() }

func (q *Queue) receiveObserved(
	ctx context.Context,
	limit int,
	observer receiveObserver,
) ([]Delivery, error) {
	if ctx == nil || limit <= 0 || limit > int(q.config.Runtime.Delivery.ReceiveItems) {
		return nil, ErrInvalidInput
	}
	q.mu.Lock()
	for {
		if err := cancellation(ctx); err != nil {
			q.mu.Unlock()
			return nil, err
		}
		if q.deliveryStopped {
			err := errors.Join(ErrStopped, q.terminal)
			q.mu.Unlock()
			return nil, err
		}
		available := 0
		for _, slot := range q.claims {
			if slot.used && !slot.acquired {
				available++
			}
		}
		if available > 0 {
			// Each existing claim reserves 256 bytes, covering its 120-byte Delivery
			// descriptor with allocator rounding before acquisition or ownership transfer.
			deliveries := make([]Delivery, 0, min(limit, available))
			for len(deliveries) < cap(deliveries) {
				count, _ := q.deliveryQueue.TryReceive(q.receiveSlot[:])
				if count == 0 {
					break
				}
				slot := &q.claims[q.receiveSlot[0]]
				slot.acquired = true
				deliveries = append(deliveries, slot.delivery)
				slot.delivery.Payload = nil
				slot.delivery.Codec = ""
			}
			if len(deliveries) > 0 {
				q.cacheStateLocked()
				q.mu.Unlock()
				if observer != nil {
					observer.Acquired()
				}
				return deliveries, nil
			}
		}
		if q.drainedLocked() {
			q.mu.Unlock()
			return nil, ErrDrained
		}
		changed := q.changed
		q.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		}
		q.mu.Lock()
	}
}

func (q *Queue) drainedLocked() bool {
	if !q.admissionStopped || q.pending != 0 {
		return false
	}
	for index := range q.ordinary.slots {
		slot := &q.ordinary.slots[index]
		if slot.used && slot.handed && slot.submit && !slot.finished {
			return false
		}
	}
	return true
}

// sourceBufferCharge bounds the supported toolchain's noscan byte allocations.
// Small classes fit A(size), and all classes fit the next 8192-byte page boundary.
// RawBytes avoids database/sql's otherwise additional scan copy.
func sourceBufferCharge(size Bytes) Bytes {
	allowance, _ := allocationCharge(size)
	return min(allowance, (size+8191)&^8191)
}

func deliveryScratchCharge(payload, codec Bytes) Bytes {
	// 1024 covers keys, digest, scan destinations and length/hash scalar scratch.
	// The codec conversion used for hashing can coexist with the source codec.
	return sourceBufferCharge(payload) + sourceBufferCharge(codec) + 2*codec + 1024
}

func (q *Queue) dispatch(ctx context.Context) error {
	for range q.config.Runtime.Delivery.ScanRows {
		q.mu.Lock()
		if q.deliveryStopped {
			q.mu.Unlock()
			return nil
		}
		free := -1
		for index := range q.claims {
			if !q.claims[index].used {
				free = index
				break
			}
		}
		if free < 0 {
			q.deliveryDirty = false
			q.scanWrapped = false
			q.mu.Unlock()
			return nil
		}
		q.mu.Unlock()
		id, exhausted, err := q.nextDeliveryID(ctx)
		if err != nil {
			return errors.Join(ErrStorage, err)
		}
		if exhausted {
			q.mu.Lock()
			q.scanOrder, q.scanID = [8]byte{}, [24]byte{}
			q.deliveryDirty = !q.scanWrapped
			q.scanWrapped = !q.scanWrapped
			q.mu.Unlock()
			return nil
		}
		q.mu.Lock()
		claimed := q.claimLocked(id) >= 0
		q.mu.Unlock()
		if claimed {
			continue
		}
		if err := q.loadDelivery(ctx, free, id); err != nil {
			return err
		}
	}
	return nil
}

func (q *Queue) nextDeliveryID(ctx context.Context) (ID, bool, error) {
	var order, encoded []byte
	err := q.opened.storage.conn.QueryRowContext(ctx, `SELECT accept_order,id FROM receipts
 WHERE state=0 AND (accept_order,id)>(?,?) ORDER BY accept_order,id LIMIT 1`,
		q.scanOrder[:], q.scanID[:]).Scan(&order, &encoded)
	if errors.Is(err, sql.ErrNoRows) {
		return ID{}, true, nil
	}
	if err != nil {
		return ID{}, false, err
	}
	if len(order) != 8 {
		return ID{}, false, ErrStorage
	}
	id, err := decodeID(encoded)
	if err != nil {
		return ID{}, false, err
	}
	copy(q.scanOrder[:], order)
	copy(q.scanID[:], encoded)
	return id, false, nil
}

func (q *Queue) loadDelivery(ctx context.Context, index int, id ID) error {
	encoded := encodeID(id)
	var payloadLength, codecLength int64
	err := q.opened.storage.conn.QueryRowContext(ctx,
		`SELECT length(payload),length(codec) FROM payloads WHERE id=?`,
		encoded[:]).Scan(&payloadLength, &codecLength)
	if err != nil {
		return errors.Join(ErrStorage, err)
	}
	if payloadLength < 0 || codecLength < 0 ||
		Bytes(payloadLength) > q.config.Policy.MaxPayload ||
		Bytes(codecLength) > q.config.Policy.MaxCodec {
		return ErrStorage
	}
	if deliveryScratchCharge(Bytes(payloadLength), Bytes(codecLength)) >
		q.config.Runtime.Delivery.ReadScratch {
		return ErrStorage
	}
	payloadCharge, _ := allocationCharge(Bytes(payloadLength))
	codecCharge, _ := allocationCharge(Bytes(codecLength))
	charge := payloadCharge + codecCharge + 256
	q.mu.Lock()
	if q.deliveryStopped {
		q.mu.Unlock()
		return nil
	}
	if charge > q.config.Runtime.Delivery.Retained-q.deliveryRetained {
		q.mu.Unlock()
		return nil
	}
	attempt, err := nextIdentity(q.lastAttempt)
	if err != nil {
		q.mu.Unlock()
		return err
	}
	q.lastAttempt = attempt
	token := encodeToken(q.opened.metadata.uuid, q.opened.metadata.epoch, id, attempt)
	q.claims[index] = claim{used: true, charge: charge, delivery: Delivery{ID: id, Token: token}}
	q.deliveryRetained += charge
	q.cacheStateLocked()
	observer := q.deliveryObserver
	q.mu.Unlock()
	if observer != nil {
		observer.BeforeDeliveryRead()
	}
	delivery, err := q.readDelivery(ctx, id, token, payloadLength, codecLength)
	q.mu.Lock()
	defer q.mu.Unlock()
	if err != nil || q.deliveryStopped {
		q.clearClaimLocked(index)
		return err
	}
	q.claims[index].delivery = delivery
	if err := q.deliveryQueue.TryPush(index); err != nil {
		q.clearClaimLocked(index)
		return errors.Join(ErrStorage, err)
	}
	q.signalLocked()
	return nil
}

func (q *Queue) readDelivery(
	ctx context.Context,
	id ID,
	token Token,
	payloadLength, codecLength int64,
) (Delivery, error) {
	encoded := encodeID(id)
	rows, err := q.opened.storage.conn.QueryContext(ctx, `SELECT p.payload,p.codec,r.digest
 FROM payloads p JOIN receipts r ON r.id=p.id WHERE p.id=?`, encoded[:])
	if err != nil {
		return Delivery{}, errors.Join(ErrStorage, err)
	}
	var payload, codec, digest sql.RawBytes
	var delivery Delivery
	if !rows.Next() {
		err = ErrStorage
	} else {
		err = rows.Scan(&payload, &codec, &digest)
		if err == nil {
			if int64(len(payload)) != payloadLength || int64(len(codec)) != codecLength {
				err = ErrStorage
			} else {
				codecCopy := string(codec)
				actual := entryDigest(codecCopy, payload)
				if !bytes.Equal(actual[:], digest) {
					err = ErrStorage
				} else {
					copied := make([]byte, len(payload))
					copy(copied, payload)
					delivery = Delivery{ID: id, Token: token, Codec: codecCopy, Payload: copied}
				}
			}
		}
	}
	err = errors.Join(err, rows.Err(), rows.Close())
	if err != nil {
		return Delivery{}, errors.Join(ErrStorage, err)
	}
	return delivery, nil
}
