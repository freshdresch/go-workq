package inventory

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

type applyStage string

const (
	provisionalReceiptInserted applyStage = "provisional receipt inserted"
	inventoryUpdated           applyStage = "inventory updated"
	destinationCommitSucceeded applyStage = "destination commit succeeded"
)

func (s *Store) observeStage(stage applyStage) {
	if s.stageHook != nil {
		s.stageHook(stage)
	}
}

const receiptWhere = `version = 1 AND namespace = ? AND producer = ?
 AND sequence = ? AND effect = ?`

func keyArguments(key Key) []any {
	sequence := binary.BigEndian.AppendUint64(nil, key.Sequence)
	return []any{key.Namespace, key.Producer[:], sequence, key.Effect}
}

func readReceipt(ctx context.Context, tx *sql.Tx, key Key) (Receipt, bool, error) {
	var codec string
	var payload []byte
	var quantity sql.NullInt64
	err := tx.QueryRowContext(ctx,
		"SELECT codec, request, result_quantity FROM receipts WHERE "+receiptWhere,
		keyArguments(key)...,
	).Scan(&codec, &payload, &quantity)
	if errors.Is(err, sql.ErrNoRows) {
		return Receipt{}, false, nil
	}
	if err != nil {
		return Receipt{}, false, err
	}
	if !quantity.Valid || quantity.Int64 < 0 {
		return Receipt{}, false, errors.New("incomplete or invalid committed inventory receipt")
	}
	encoded, err := key.Encode()
	if err != nil {
		return Receipt{}, false, err
	}
	return Receipt{
		Key: key, Codec: codec, Payload: bytes.Clone(payload),
		Result: Result{AdjustmentID: encoded, Quantity: quantity.Int64},
	}, true, nil
}

// Apply atomically records request and adjusts widget once.
// Matching retries return the saved result.
// Conflict, rejection, and capacity errors establish no mutation by this invocation only after
// successful cleanup. ErrUnresolved requires the owner to stop and reconcile after close/reopen.
func (s *Store) Apply(ctx context.Context, request Request) (Result, error) {
	s.lockOperation()
	defer s.opMu.Unlock()
	if err := s.check(ctx); err != nil {
		return Result{}, err
	}
	if err := request.validateEnvelope(); err != nil {
		return Result{}, err
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("begin apply: %w", err)
	}
	result, err := s.apply(ctx, tx, request)
	if err != nil {
		return Result{}, s.rollback(tx, err)
	}
	if err := s.commit(ctx, tx); err != nil {
		return Result{}, err
	}
	s.observeStage(destinationCommitSucceeded)
	return result, nil
}

func (s *Store) apply(ctx context.Context, tx *sql.Tx, request Request) (Result, error) {
	receipt, found, err := readReceipt(ctx, tx, request.Key)
	if err != nil {
		return Result{}, fmt.Errorf("read receipt: %w: %w", ErrUnresolved, err)
	}
	if found {
		if receipt.Codec != request.Codec || !bytes.Equal(receipt.Payload, request.Payload) {
			return Result{}, ErrConflict
		}
		result := receipt.Result
		result.Duplicate = true
		return result, nil
	}
	adjustment, err := request.adjustment()
	if err != nil {
		return Result{}, err
	}
	var count int
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM receipts").Scan(&count); err != nil {
		return Result{}, fmt.Errorf("receipt count: %w: %w", ErrUnresolved, err)
	}
	if count >= s.receiptLimit {
		return Result{}, ErrCapacity
	}
	args := keyArguments(request.Key)
	args = append(args, request.Codec, request.Payload)
	_, err = tx.ExecContext(ctx, `INSERT INTO receipts
  (version, namespace, producer, sequence, effect, codec, request)
  VALUES (1, ?, ?, ?, ?, ?, ?)`, args...)
	if err != nil {
		return Result{}, fmt.Errorf("insert receipt: %w: %w", ErrUnresolved, err)
	}
	s.observeStage(provisionalReceiptInserted)
	var current Inventory
	err = tx.QueryRowContext(ctx,
		"SELECT quantity, adjustments FROM inventory WHERE item = ?", adjustment.item,
	).Scan(&current.Quantity, &current.Adjustments)
	if errors.Is(err, sql.ErrNoRows) {
		return Result{}, ErrRejected
	}
	if err != nil {
		return Result{}, fmt.Errorf("read inventory: %w: %w", ErrUnresolved, err)
	}
	quantity, err := adjustedQuantity(current.Quantity, adjustment.delta)
	if err != nil {
		return Result{}, err
	}
	if current.Adjustments < 0 || current.Adjustments == math.MaxInt64 {
		return Result{}, fmt.Errorf("adjustment count out of range: %w", ErrRejected)
	}
	updated, err := tx.ExecContext(ctx,
		"UPDATE inventory SET quantity = ?, adjustments = ? WHERE item = ?",
		quantity, current.Adjustments+1, adjustment.item,
	)
	if err := exactlyOne(updated, err); err != nil {
		return Result{}, err
	}
	s.observeStage(inventoryUpdated)
	args = append([]any{quantity}, keyArguments(request.Key)...)
	updated, err = tx.ExecContext(ctx,
		"UPDATE receipts SET result_quantity = ? WHERE "+receiptWhere, args...)
	if err := exactlyOne(updated, err); err != nil {
		return Result{}, err
	}
	encoded, err := request.Key.Encode()
	if err != nil {
		return Result{}, err
	}
	return Result{AdjustmentID: encoded, Quantity: quantity}, nil
}

func exactlyOne(result sql.Result, err error) error {
	if err != nil {
		return fmt.Errorf("update inventory transaction: %w: %w", ErrUnresolved, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("affected rows: %w: %w", ErrUnresolved, err)
	}
	if affected != 1 {
		return fmt.Errorf("affected %d rows, want 1: %w", affected, ErrUnresolved)
	}
	return nil
}

// Lookup reads a complete receipt in an immediate transaction.
// Absence is usable only with nil error.
// It returns detached payload bytes and never changes a receipt's original result.
func (s *Store) Lookup(ctx context.Context, key Key) (Receipt, bool, error) {
	s.lockOperation()
	defer s.opMu.Unlock()
	if err := s.check(ctx); err != nil {
		return Receipt{}, false, err
	}
	if err := key.validate(); err != nil {
		return Receipt{}, false, err
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return Receipt{}, false, fmt.Errorf("begin lookup: %w", err)
	}
	receipt, found, err := readReceipt(ctx, tx, key)
	if err != nil {
		return Receipt{}, false, s.rollback(tx, fmt.Errorf("lookup: %w: %w", ErrUnresolved, err))
	}
	if err := s.commit(ctx, tx); err != nil {
		return Receipt{}, false, err
	}
	return receipt, found, nil
}

// Snapshot reads the persistent widget quantity and committed adjustment count.
func (s *Store) Snapshot(ctx context.Context) (Inventory, error) {
	s.lockOperation()
	defer s.opMu.Unlock()
	if err := s.check(ctx); err != nil {
		return Inventory{}, err
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return Inventory{}, fmt.Errorf("begin snapshot: %w", err)
	}
	var inventory Inventory
	err = tx.QueryRowContext(ctx,
		"SELECT quantity, adjustments FROM inventory WHERE item = 'widget'",
	).Scan(&inventory.Quantity, &inventory.Adjustments)
	if err != nil {
		return Inventory{}, s.rollback(tx, fmt.Errorf("snapshot: %w: %w", ErrUnresolved, err))
	}
	if err := s.commit(ctx, tx); err != nil {
		return Inventory{}, err
	}
	return inventory, nil
}
