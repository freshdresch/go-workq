package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/bits"
	"sync"
	"sync/atomic"
	"time"

	"github.com/freshdresch/go-workq/durable"
)

const (
	producerCount = 4
	creditWindow  = 256
	producerQuota = 2048
	waveLimit     = producerCount * producerQuota
	codec         = "comparison-v1"
	drainTimeout  = 30 * time.Second
)

var errDrainTimeout = errors.New("harness drain watchdog expired")

type latency struct {
	Count   uint64        `json:"count"`
	Total   time.Duration `json:"total_ns"`
	Max     time.Duration `json:"max_ns"`
	Buckets [64]uint64    `json:"base2_nanosecond_buckets"`
}

func (l *latency) add(duration time.Duration) {
	l.Count++
	l.Total += duration
	l.Max = max(l.Max, duration)
	// Bucket zero includes zero and one nanosecond. Bucket k covers [2^k,2^(k+1)).
	bucket := max(0, bits.Len64(uint64(duration))-1)
	l.Buckets[bucket]++
}

func (l *latency) merge(other latency) {
	l.Count += other.Count
	l.Total += other.Total
	l.Max = max(l.Max, other.Max)
	for index, count := range other.Buckets {
		l.Buckets[index] += count
	}
}

type waveEvidence struct {
	Base             [producerCount]uint64           `json:"base_sequences"`
	Entered          [producerCount]uint64           `json:"entered_prefixes"`
	Accepted         [producerCount]uint64           `json:"accepted_prefixes"`
	Received         [waveLimit / 64]uint64          `json:"received_bits"`
	Acked            [waveLimit / 64]uint64          `json:"acknowledged_bits"`
	LastReceived     [creditWindow]durable.ID        `json:"last_received_ids"`
	LastReceiveCount int                             `json:"last_receive_count"`
	SubmitDecision   [producerCount]durable.Decision `json:"last_submit_decisions"`
	AckDecision      durable.Decision                `json:"last_ack_decision"`
}

type phaseStats struct {
	FailedWave         *waveEvidence            `json:"failed_wave,omitempty"`
	AcceptedByProducer [producerCount]uint64    `json:"accepted_by_producer"`
	Accepted           uint64                   `json:"accepted"`
	Acknowledged       uint64                   `json:"acknowledged"`
	Waves              uint64                   `json:"waves"`
	Submit             latency                  `json:"submit_batch_latency"`
	Ack                latency                  `json:"ack_batch_latency"`
	ReceiveSizes       [creditWindow + 1]uint64 `json:"receive_batch_sizes"`
	AckSizes           [creditWindow + 1]uint64 `json:"ack_batch_sizes"`
}

type workload struct {
	batch     int
	payloads  [producerCount][]byte
	digests   [producerCount][sha256.Size]byte
	entries   [producerCount][creditWindow]durable.Entry
	tokens    [creditWindow]durable.Token
	frontiers [producerCount]uint64
}

func producerID(producer int) [16]byte {
	var id [16]byte
	id[0] = byte(producer + 1)
	return id
}

func newWorkload(opts options) *workload {
	work := &workload{batch: opts.Batch}
	for producer := range producerCount {
		payload := make([]byte, opts.Payload)
		for index := range payload {
			payload[index] = byte((index*31 + producer*47) % 251)
		}
		work.payloads[producer] = payload
		work.digests[producer] = sha256.Sum256(payload)
		for index := range creditWindow {
			work.entries[producer][index] = durable.Entry{
				ID: durable.ID{Producer: producerID(producer)}, Codec: codec, Payload: payload,
			}
		}
	}
	return work
}

type wave struct {
	base             [producerCount]uint64
	entered          [producerCount]atomic.Uint64
	accepted         [producerCount]uint64
	submit           [producerCount]latency
	credits          [producerCount]chan struct{}
	oracleMu         sync.Mutex
	seen             [waveLimit / 64]uint64
	acked            [waveLimit / 64]uint64
	lastReceived     [creditWindow]durable.ID
	lastReceiveCount int
	submitDecision   [producerCount]durable.Decision
	ackDecision      durable.Decision
	acknowledged     atomic.Uint64
	ack              latency
	receiveSizes     [creditWindow + 1]uint64
	ackSizes         [creditWindow + 1]uint64
	progress         chan struct{}
	stop             chan struct{}
	failure          chan error
	once             sync.Once
}

func newWave(frontiers [producerCount]uint64) *wave {
	current := &wave{
		progress: make(chan struct{}, 1), stop: make(chan struct{}), failure: make(chan error, 1),
	}
	for producer := range producerCount {
		current.base[producer] = frontiers[producer] + 1
		// Each channel represents this producer's fixed outstanding-item budget.
		current.credits[producer] = make(chan struct{}, creditWindow)
		for range creditWindow {
			current.credits[producer] <- struct{}{}
		}
	}
	return current
}

func (w *wave) fail(queue *durable.Queue, err error) {
	w.once.Do(func() {
		w.failure <- err
		close(w.stop)
		queue.Abort(err)
	})
}

func reserveCredits(
	credits <-chan struct{},
	count int,
	stop <-chan struct{},
	deadline time.Time,
) int {
	reserved := 0
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	for reserved < count {
		if !time.Now().Before(deadline) {
			return reserved
		}
		select {
		case <-stop:
			return reserved
		case <-timer.C:
			return reserved
		case <-credits:
			reserved++
		}
	}
	return reserved
}

func returnCredits(credits chan<- struct{}, count int) {
	for range count {
		credits <- struct{}{}
	}
}

func (w *workload) produce(queue *durable.Queue, current *wave, producer int, deadline time.Time) {
	for current.accepted[producer] < producerQuota {
		if !time.Now().Before(deadline) {
			return
		}
		count := min(w.batch, producerQuota-int(current.accepted[producer]))
		reserved := reserveCredits(current.credits[producer], count, current.stop, deadline)
		select {
		case <-current.stop:
			returnCredits(current.credits[producer], reserved)
			return
		default:
		}
		if reserved != count || !time.Now().Before(deadline) {
			returnCredits(current.credits[producer], reserved)
			return
		}
		first := current.accepted[producer]
		entries := w.entries[producer][:count]
		for index := range entries {
			entries[index].ID.Sequence = current.base[producer] + first + uint64(index)
		}
		if !time.Now().Before(deadline) {
			returnCredits(current.credits[producer], reserved)
			return
		}
		current.entered[producer].Store(first + uint64(count))
		started := time.Now()
		committed, err := queue.SubmitBatch(context.Background(), entries)
		current.submit[producer].add(time.Since(started))
		current.submitDecision[producer] = committed.Decision
		if err != nil || committed.Decision != durable.Applied {
			current.fail(queue, fmt.Errorf("submit producer %d: %s: %w", producer,
				committed.Decision, errors.Join(err, errors.New("submission not confirmed"))))
			return
		}
		current.accepted[producer] += uint64(count)
	}
}

func (w *wave) validate(
	delivery durable.Delivery,
	digests [producerCount][sha256.Size]byte,
) (int, error) {
	w.oracleMu.Lock()
	defer w.oracleMu.Unlock()
	producer := int(delivery.ID.Producer[0]) - 1
	if producer < 0 || producer >= producerCount || delivery.ID.Producer != producerID(producer) {
		return 0, fmt.Errorf("unknown producer: %x", delivery.ID.Producer)
	}
	if delivery.ID.Sequence < w.base[producer] {
		return 0, errors.New("ID below wave base")
	}
	offset := delivery.ID.Sequence - w.base[producer]
	if offset >= producerQuota || offset >= w.entered[producer].Load() {
		return 0, fmt.Errorf("ID outside entered prefix: %+v", delivery.ID)
	}
	bit := uint64(producer*producerQuota) + offset
	mask := uint64(1) << (bit % 64)
	if w.seen[bit/64]&mask != 0 {
		return 0, fmt.Errorf("duplicate ID: %+v", delivery.ID)
	}
	if delivery.Codec != codec || sha256.Sum256(delivery.Payload) != digests[producer] {
		return 0, fmt.Errorf("payload or codec mismatch: %+v", delivery.ID)
	}
	w.seen[bit/64] |= mask
	return producer, nil
}

func (w *workload) consume(ctx context.Context, queue *durable.Queue, current *wave) {
	for {
		deliveries, err := queue.Receive(ctx, w.batch)
		if errors.Is(err, context.Canceled) {
			return
		}
		if err != nil {
			current.fail(queue, fmt.Errorf("receive: %w", err))
			return
		}
		if len(deliveries) == 0 || len(deliveries) > w.batch {
			current.fail(queue, errors.New("invalid receive count"))
			return
		}
		current.lastReceiveCount = len(deliveries)
		clear(current.lastReceived[:])
		for index, delivery := range deliveries {
			current.lastReceived[index] = delivery.ID
		}
		current.receiveSizes[len(deliveries)]++
		var released [producerCount]int
		for index, delivery := range deliveries {
			producer, err := current.validate(delivery, w.digests)
			if err != nil {
				current.fail(queue, err)
				return
			}
			released[producer]++
			w.tokens[index] = delivery.Token
		}
		started := time.Now()
		committed, err := queue.AckBatch(context.Background(), w.tokens[:len(deliveries)])
		current.ack.add(time.Since(started))
		current.ackDecision = committed.Decision
		if err != nil || committed.Decision != durable.Applied {
			current.fail(queue, fmt.Errorf("ack: %s: %w", committed.Decision,
				errors.Join(err, errors.New("acknowledgment not confirmed"))))
			return
		}
		current.ackSizes[len(deliveries)]++
		for _, delivery := range deliveries {
			producer := int(delivery.ID.Producer[0]) - 1
			bit := uint64(producer*producerQuota) + delivery.ID.Sequence - current.base[producer]
			current.acked[bit/64] |= uint64(1) << (bit % 64)
		}
		for producer, count := range released {
			returnCredits(current.credits[producer], count)
		}
		current.acknowledged.Add(uint64(len(deliveries)))
		select {
		case current.progress <- struct{}{}:
		default:
		}
	}
}

func (w *wave) checkComplete() error {
	w.oracleMu.Lock()
	defer w.oracleMu.Unlock()
	var accepted uint64
	for producer, count := range w.accepted {
		accepted += count
		if w.entered[producer].Load() != count {
			return errors.New("entered/accepted prefix mismatch")
		}
		if len(w.credits[producer]) != creditWindow {
			return errors.New("unreturned admission credit")
		}
		for offset := range producerQuota {
			bit := producer*producerQuota + offset
			seen := w.seen[bit/64]&(uint64(1)<<uint(bit%64)) != 0
			if seen != (uint64(offset) < count) {
				return errors.New("exact received ID set mismatch")
			}
		}
	}
	if accepted != w.acknowledged.Load() {
		return errors.New("accepted/Ack count mismatch")
	}
	return nil
}

func (w *wave) waitDrain(timeout time.Duration) error {
	var accepted uint64
	for _, count := range w.accepted {
		accepted += count
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for w.acknowledged.Load() < accepted {
		select {
		case <-w.stop:
			return <-w.failure
		case <-timer.C:
			return fmt.Errorf("%w: accepted %d, acknowledged %d",
				errDrainTimeout, accepted, w.acknowledged.Load())
		case <-w.progress:
		}
	}
	select {
	case <-w.stop:
		return <-w.failure
	default:
		return nil
	}
}

func (w *workload) runWave(queue *durable.Queue, deadline time.Time) (*wave, error) {
	current := newWave(w.frontiers)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumerDone := make(chan struct{})
	go func() { defer close(consumerDone); w.consume(ctx, queue, current) }()
	var producers sync.WaitGroup
	for producer := range producerCount {
		producers.Go(func() { w.produce(queue, current, producer, deadline) })
	}
	producers.Wait()
	if err := current.waitDrain(drainTimeout); err != nil {
		current.fail(queue, err)
		<-consumerDone
		return current, err
	}
	// Check the exact accepted set before canceling a Receive that owns no work.
	if err := current.checkComplete(); err != nil {
		current.fail(queue, err)
		<-consumerDone
		return current, <-current.failure
	}
	cancel()
	<-consumerDone
	select {
	case err := <-current.failure:
		return current, err
	default:
	}
	return current, current.checkComplete()
}

func (w *workload) phase(queue *durable.Queue, duration time.Duration) (phaseStats, error) {
	var stats phaseStats
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		current, err := w.runWave(queue, deadline)
		stats.mergeWave(current)
		if err != nil {
			stats.FailedWave = current.evidence()
			return stats, err
		}
		for producer, count := range current.accepted {
			w.frontiers[producer] += count
			committed, err := queue.RetireThrough(context.Background(),
				producerID(producer), w.frontiers[producer])
			if err != nil || committed.Decision != durable.Applied {
				stats.FailedWave = current.evidence()
				return stats, fmt.Errorf("retire producer %d: %s: %w", producer,
					committed.Decision, errors.Join(err, errors.New("retirement not confirmed")))
			}
		}
		if err := waitCleanup(queue, drainTimeout); err != nil {
			stats.FailedWave = current.evidence()
			return stats, err
		}
	}
	return stats, nil
}

func waitCleanup(queue *durable.Queue, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		snapshot := queue.Snapshot()
		if snapshot.State != durable.Active {
			return fmt.Errorf("cleanup state: %s", snapshot.State)
		}
		counts := snapshot.Store.Counts
		if counts.PendingItems != 0 || counts.PendingPayload != 0 {
			return errors.New("pending work after exact Ack drain")
		}
		if counts.Receipts == 0 && counts.CleanupMarkers == 0 {
			return nil
		}
		select {
		case <-timer.C:
			return errors.New("harness cleanup watchdog expired")
		case <-queue.Done():
			return errors.Join(errors.New("queue closed during cleanup"), queue.Wait())
		case <-ticker.C:
		}
	}
}

// mergeWave runs only after all wave workers join, including on failure.
func (s *phaseStats) mergeWave(current *wave) {
	s.Waves++
	s.Acknowledged += current.acknowledged.Load()
	s.Ack.merge(current.ack)
	for index := range s.ReceiveSizes {
		s.ReceiveSizes[index] += current.receiveSizes[index]
		s.AckSizes[index] += current.ackSizes[index]
	}
	for producer, count := range current.accepted {
		s.Accepted += count
		s.AcceptedByProducer[producer] += count
		s.Submit.merge(current.submit[producer])
	}
}

// evidence retains only the joined wave, so failure diagnostics stay bounded.
func (w *wave) evidence() *waveEvidence {
	evidence := &waveEvidence{
		Base: w.base, Accepted: w.accepted, Received: w.seen, Acked: w.acked,
		LastReceived: w.lastReceived, LastReceiveCount: w.lastReceiveCount,
		SubmitDecision: w.submitDecision, AckDecision: w.ackDecision,
	}
	for producer := range producerCount {
		evidence.Entered[producer] = w.entered[producer].Load()
	}
	return evidence
}
