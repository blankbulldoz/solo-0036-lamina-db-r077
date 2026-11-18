package engine

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"lamina/internal/datum"
	"lamina/internal/journal"
	"lamina/internal/versions"
	"time"
)

// transactionOp is one staged edit in stage order. Single-row operations
// replace earlier stood-alone single edits of the same key; batch plans are
// deferred predicates that see the private view produced by all earlier
// operations. Keeping stage order makes single and batch edits compose.
type transactionOp struct {
	single *versions.Mutation
	batch  *batchPlan
	// size is the encoded journal size of a single mutation, computed once so
	// admission stays incremental for the common row-by-row path.
	size int
}

type Transaction struct {
	ID       string `json:"id"`
	Snapshot uint64 `json:"snapshot"`
	ReadOnly bool   `json:"read_only"`
	Started  string `json:"started"`
	ops      []transactionOp
}
type Commit struct {
	Sequence uint64 `json:"sequence"`
	Writes   int    `json:"writes"`
}

func (d *DB) Begin(readOnly bool) (Transaction, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return Transaction{}, err
	}
	if len(d.transactions) >= 128 {
		return Transaction{}, fault("capacity", "128 active transactions reached")
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return Transaction{}, err
	}
	tx := &Transaction{ID: hex.EncodeToString(id[:]), Snapshot: d.sequence, ReadOnly: readOnly, Started: time.Now().UTC().Format(time.RFC3339Nano)}
	d.transactions[tx.ID] = tx
	out := *tx
	out.ops = nil
	return out, nil
}
func (d *DB) tx(id string) (*Transaction, error) {
	tx, ok := d.transactions[id]
	if !ok {
		return nil, fault("not_found", "transaction not active")
	}
	return tx, nil
}

// withSingle returns a copy of the operation log carrying one additional
// single-row edit. A prior edit of the same key after the last batch is
// replaced; otherwise the edit is appended. The original log is untouched so a
// failed capacity admission can discard the candidate without side effects.
func withSingle(ops []transactionOp, mutation versions.Mutation, size int) []transactionOp {
	lastBatch := -1
	for index := len(ops) - 1; index >= 0; index-- {
		if ops[index].batch != nil {
			lastBatch = index
			break
		}
	}
	out := make([]transactionOp, len(ops))
	copy(out, ops)
	for index := lastBatch + 1; index < len(out); index++ {
		current := out[index].single
		if current != nil && current.Relation == mutation.Relation && current.Key == mutation.Key {
			replacement := mutation
			out[index] = transactionOp{single: &replacement, size: size}
			return out
		}
	}
	candidate := mutation
	return append(out, transactionOp{single: &candidate, size: size})
}

func mutationSize(mutation versions.Mutation) int {
	encoded, err := json.Marshal(mutation)
	if err != nil {
		return journal.MaxFrame + 1
	}
	return len(encoded)
}

func (d *DB) Put(id, table string, row datum.Row, insertOnly bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return err
	}
	tx, err := d.tx(id)
	if err != nil {
		return err
	}
	if tx.ReadOnly {
		return fault("conflict", "read-only transaction")
	}
	r, err := d.batchRelation(tx, table)
	if err != nil {
		return err
	}
	normalized, err := r.Normalize(row)
	if err != nil {
		return invalid(err)
	}
	key := r.Key(normalized)
	if insertOnly {
		if _, ok := d.txView(tx, table)[key]; ok {
			return fault("conflict", "primary already exists")
		}
	}
	if err = d.stage(tx, withSingle(tx.ops, versions.Mutation{Relation: table, Key: key, Row: normalized}, mutationSize(versions.Mutation{Relation: table, Key: key, Row: normalized}))); err != nil {
		return err
	}
	return nil
}

func (d *DB) Delete(id, table string, value datum.Value) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return err
	}
	tx, err := d.tx(id)
	if err != nil {
		return err
	}
	if tx.ReadOnly {
		return fault("conflict", "read-only transaction")
	}
	r, err := d.batchRelation(tx, table)
	if err != nil {
		return err
	}
	col, _ := r.Column(r.Primary)
	if value.Null() || value.Kind != col.Type {
		return fault("invalid", "invalid primary value")
	}
	if err = d.stage(tx, withSingle(tx.ops, versions.Mutation{Relation: table, Key: value.Key(), Deleted: true}, mutationSize(versions.Mutation{Relation: table, Key: value.Key(), Deleted: true}))); err != nil {
		return err
	}
	return nil
}

// stage admits a candidate operation log against the exact write-set and
// journal-frame budgets, then installs it. A rejection leaves the previous log
// and every earlier single edit intact; it never consumes a publish sequence.
//
// When the transaction still contains only single-row edits, the budget is
// measured from cached per-mutation sizes, so a 1000-row loop costs linear
// time. A staged batch may change existing keys, so any later edit re-derives
// the final set exactly before it can be installed.
func (d *DB) stage(tx *Transaction, candidate []transactionOp) error {
	previous := tx.ops
	tx.ops = candidate
	if singleOnly(candidate) {
		// The final write set replaces an earlier edit of the same key, and
		// withSingle already does that replacement. Every surviving operation is
		// one final mutation, so cached encoded sizes give the exact budget
		// without re-serializing the transaction.
		count := 0
		encoded := 0
		for index := range candidate {
			if candidate[index].size == 0 {
				candidate[index].size = mutationSize(*candidate[index].single)
			}
			count++
			encoded += candidate[index].size
		}
		capacity := BatchCapacity{
			OK:                 true,
			WriteSetLimit:      MaxTransactionMutations,
			WriteSetUsed:       count,
			Changes:            count,
			ProjectedMutations: count,
			JournalLimit:       journal.MaxFrame,
		}
		if count > MaxTransactionMutations {
			capacity.OK = false
			capacity.Reason = "write_set"
		}
		capacity.JournalBytes = d.commitHeaderSize(tx) + journal.WritesFieldOverhead + encoded + count
		if capacity.JournalBytes+batchFrameMargin > journal.MaxFrame {
			capacity.OK = false
			if capacity.Reason == "" {
				capacity.Reason = "journal_frame"
			}
		}
		if !capacity.OK {
			tx.ops = previous
			return capacityError(capacity)
		}
		return nil
	}
	mutations, err := d.txWrites(tx, nil)
	if err != nil {
		tx.ops = previous
		return err
	}
	capacity := d.capacityFor(tx, flatten(mutations))
	if !capacity.OK {
		tx.ops = previous
		return capacityError(capacity)
	}
	return nil
}

func singleOnly(ops []transactionOp) bool {
	for index := range ops {
		if ops[index].batch != nil {
			return false
		}
	}
	return true
}

// commitHeaderSize is the encoded size of the commit record with no writes,
// using the impending sequence so digit growth is accounted for.
func (d *DB) commitHeaderSize(tx *Transaction) int {
	base, _, err := journal.WireSizes(journal.Record{Format: 1, Sequence: d.sequence + 1, Kind: "commit", Transaction: tx.ID})
	if err != nil {
		return journal.MaxFrame
	}
	return base
}

func (d *DB) Rollback(id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return err
	}
	if _, err := d.tx(id); err != nil {
		return err
	}
	delete(d.transactions, id)
	return nil
}

// Commit expands deferred plans exactly once, validates them against the
// catalog and committed state current at publish time, and writes one durable
// record. Any failure closes the transaction without publishing a partial set.
func (d *DB) Commit(id string) (Commit, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return Commit{}, err
	}
	tx, err := d.tx(id)
	if err != nil {
		return Commit{}, err
	}
	defer delete(d.transactions, id)
	if err = d.verifyPlanDigests(tx); err != nil {
		return Commit{}, err
	}
	mutations, err := d.txWrites(tx, nil)
	if err != nil {
		return Commit{}, err
	}
	for _, perKey := range mutations {
		for _, mutation := range perKey {
			if d.store.Latest(mutation.Relation, mutation.Key) > tx.Snapshot {
				d.conflicts++
				return Commit{}, fault("conflict", "write conflict on %s", mutation.Relation)
			}
		}
	}
	if err = d.uniqueFinal(mutations); err != nil {
		d.conflicts++
		return Commit{}, err
	}
	writes := flatten(mutations)
	if len(writes) == 0 {
		return Commit{Sequence: tx.Snapshot}, nil
	}
	// This is the authoritative check. Admission already measured the same
	// final write set, but commit measures the exact record it is about to
	// frame, including the real sequence digits, so a capacity rejection here
	// cannot be bypassed by staging order or header growth.
	record := journal.Record{Format: 1, Sequence: d.sequence + 1, Kind: "commit", Transaction: id, Writes: writes}
	if size, within, err := journal.PayloadSizeWithin(record, journal.MaxFrame); err != nil {
		return Commit{}, err
	} else if !within || len(writes) > MaxTransactionMutations {
		capacity := d.capacityFor(tx, writes)
		capacity.OK = false
		capacity.JournalBytes = size
		if len(writes) > MaxTransactionMutations {
			capacity.Reason = "write_set"
		} else {
			capacity.Reason = "journal_frame"
		}
		return Commit{}, capacityError(capacity)
	}
	if err = d.publish(record); err != nil {
		return Commit{}, err
	}
	d.commits++
	return Commit{Sequence: d.sequence, Writes: len(writes)}, nil
}

// verifyPlanDigests guards the promise that a previewed selection is the set
// that publishes. Expansion is deterministic under the transaction snapshot,
// so a mismatch means private state changed unexpectedly; failing the whole
// transaction is the only safe outcome.
func (d *DB) verifyPlanDigests(tx *Transaction) error {
	for index := range tx.ops {
		plan := tx.ops[index].batch
		if plan == nil {
			continue
		}
		view := d.viewBefore(tx, index, plan.Relation)
		matched, changes, digest, err := expandOn(plan.relation(), plan, view)
		if err != nil {
			return err
		}
		if digest != plan.Digest || matched != plan.Matched || len(changes) != plan.Changes {
			d.conflicts++
			return &Error{Code: "conflict", Message: "staged batch selection changed since preview", Details: map[string]any{"reason": "selection_changed", "want_digest": plan.Digest, "got_digest": digest, "want_matched": plan.Matched, "got_matched": matched, "want_changes": plan.Changes, "got_changes": len(changes)}}
		}
	}
	return nil
}

// viewBefore builds the private row view for a relation immediately before the
// operation at index.
func (d *DB) viewBefore(tx *Transaction, index int, relation string) map[string]datum.Row {
	rows := d.store.Scan(relation, tx.Snapshot)
	for prior := 0; prior < index; prior++ {
		op := &tx.ops[prior]
		if op.single != nil && op.single.Relation == relation {
			applyMutation(rows, *op.single)
			continue
		}
		if op.batch == nil || op.batch.Relation != relation {
			continue
		}
		if _, changes, _, err := expandOn(op.batch.relation(), op.batch, rows); err == nil {
			for _, change := range changes {
				applyMutation(rows, change.Mutation)
			}
		}
	}
	return rows
}

func (d *DB) capacityFor(tx *Transaction, writes []versions.Mutation) BatchCapacity {
	capacity := BatchCapacity{
		OK:                 true,
		WriteSetLimit:      MaxTransactionMutations,
		WriteSetUsed:       len(writes),
		Changes:            len(writes),
		ProjectedMutations: len(writes),
		JournalBytes:       d.frameSize(tx, writes),
		JournalLimit:       journal.MaxFrame,
	}
	if len(writes) > MaxTransactionMutations {
		capacity.OK = false
		capacity.Reason = "write_set"
	}
	// Admission keeps a fixed reserve for sequence and header growth between
	// staging and the durable write; commit recomputes the exact size.
	if capacity.JournalBytes+batchFrameMargin > journal.MaxFrame {
		capacity.OK = false
		if capacity.Reason == "" {
			capacity.Reason = "journal_frame"
		}
	}
	return capacity
}
