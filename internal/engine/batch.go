package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"lamina/internal/catalog"
	"lamina/internal/datum"
	"lamina/internal/journal"
	"lamina/internal/query"
	"lamina/internal/versions"
	"sort"
)

const (
	// MaxTransactionMutations is the per-transaction final write-set budget.
	// Batches are admitted against the same budget as single-row edits, so a
	// transaction can never become an unbounded write set.
	MaxTransactionMutations = 1000
	// Preview echo is bounded independently of the selection size. Matched and
	// changes always describe the complete filter result.
	DefaultBatchPreviewRows = 100
	MaxBatchPreviewRows     = 1000
	// Admission reserves this many payload bytes for sequence digits and other
	// header growth between preview and the durable write. Commit rechecks the
	// exact frame size before publishing.
	batchFrameMargin    = 64
	maxPreviewConflicts = 16
)

// batchPlan is a transaction-private description of a filtered adjustment.
// It stores the predicate, not an expanded row list: staging a wide range costs
// one small plan rather than one write-set entry per matching row. The
// selection is frozen by the transaction snapshot and is expanded only when a
// preview, a private read, or commit needs concrete mutations. Expansions are
// transient; nothing per-row is retained until the durable commit record.
type batchPlan struct {
	Relation string
	Where    []query.Predicate
	Set      datum.Row
	Delete   bool
	Primary  string
	Columns  []catalog.Column
	// Digest, Matched and Changes froze the previewed selection. Commit
	// re-expands the plan at the same snapshot and refuses to publish unless
	// all three still agree.
	Digest  string
	Matched int
	Changes int
}

func (p *batchPlan) relation() catalog.Relation {
	return catalog.Relation{Name: p.Relation, Primary: p.Primary, Columns: p.Columns}
}

type BatchRequest struct {
	Relation string            `json:"relation"`
	Where    []query.Predicate `json:"where,omitempty"`
	Set      datum.Row         `json:"set,omitempty"`
	Delete   bool              `json:"delete,omitempty"`
	Preview  string            `json:"preview,omitempty"`
	Limit    int               `json:"limit,omitempty"`
}

type BatchRow struct {
	Key     string    `json:"key"`
	Before  datum.Row `json:"before,omitempty"`
	After   datum.Row `json:"after,omitempty"`
	Deleted bool      `json:"deleted,omitempty"`
}

// BatchConflict reports a condition that makes the staged adjustment fail at
// commit under current committed state and catalog. It is advisory: the
// preview is not a reservation and does not serialize concurrent writers.
type BatchConflict struct {
	Kind    string `json:"kind"`
	Key     string `json:"key,omitempty"`
	Index   string `json:"index,omitempty"`
	Message string `json:"message"`
}

// BatchCapacity states exactly which budget the adjustment is measured
// against. Reason is empty when both budgets fit.
type BatchCapacity struct {
	OK                 bool   `json:"ok"`
	Reason             string `json:"reason,omitempty"`
	WriteSetLimit      int    `json:"write_set_limit"`
	WriteSetUsed       int    `json:"write_set_used"`
	Changes            int    `json:"changes"`
	ProjectedMutations int    `json:"projected_mutations"`
	JournalBytes       int    `json:"journal_bytes"`
	JournalLimit       int    `json:"journal_limit"`
}

type BatchPreview struct {
	Relation         string          `json:"relation"`
	Snapshot         uint64          `json:"snapshot"`
	Head             uint64          `json:"head"`
	Matched          int             `json:"matched"`
	Changes          int             `json:"changes"`
	Rows             []BatchRow      `json:"rows"`
	Truncated        bool            `json:"truncated"`
	Token            string          `json:"token"`
	Staged           bool            `json:"staged"`
	Feasible         bool            `json:"feasible"`
	Conflicts        []BatchConflict `json:"conflicts"`
	Capacity         BatchCapacity   `json:"capacity"`
	UniqueIndexes    []string        `json:"unique_indexes"`
	CatalogSignature string          `json:"catalog_signature"`
	Note             string          `json:"note"`
}

type batchChange struct {
	Mutation versions.Mutation
	Before   datum.Row
	After    datum.Row
}

// batchRelation returns a relation visible to the transaction snapshot. DDL
// committed after the snapshot is not part of transaction-private editing,
// matching single-row Put/Delete behavior.
func (d *DB) batchRelation(tx *Transaction, name string) (catalog.Relation, error) {
	r, err := d.catalog.Relation(name)
	if err != nil {
		return r, invalid(err)
	}
	if r.Created > tx.Snapshot {
		return r, fault("conflict", "relation outside transaction catalog snapshot")
	}
	return r, nil
}

func validateBatchRequest(r catalog.Relation, req BatchRequest) (batchPlan, error) {
	plan := batchPlan{
		Relation: r.Name,
		Where:    append([]query.Predicate(nil), req.Where...),
		Set:      datum.Clone(req.Set),
		Delete:   req.Delete,
		Primary:  r.Primary,
		Columns:  append([]catalog.Column(nil), r.Columns...),
	}
	if req.Limit < 0 || req.Limit > MaxBatchPreviewRows {
		return plan, fault("invalid", "preview limit must be 0..%d", MaxBatchPreviewRows)
	}
	if req.Delete && len(req.Set) > 0 {
		return plan, fault("invalid", "delete and set are mutually exclusive")
	}
	if !req.Delete && len(req.Set) == 0 {
		return plan, fault("invalid", "batch adjustment needs set or delete")
	}
	if err := query.Bind(r, query.Request{Relation: r.Name, Where: req.Where, Limit: 1}); err != nil {
		return plan, invalid(err)
	}
	for name, value := range req.Set {
		column, ok := r.Column(name)
		if !ok {
			return plan, fault("invalid", "unknown set column %s", name)
		}
		if name == r.Primary {
			return plan, fault("invalid", "primary key is immutable; a batch cannot rename rows")
		}
		if value.Null() && !column.Nullable {
			return plan, fault("invalid", "column %s cannot be null", name)
		}
		if !value.Null() && value.Kind != column.Type {
			return plan, fault("invalid", "column %s requires %s", name, column.Type)
		}
	}
	return plan, nil
}

// expandOn evaluates the plan over the supplied private view. Rows whose final
// value equals the before value are not changes and cost no write-set entry.
// The digest covers exactly the remaining key/before/after triples, so commit
// can prove the published set is the previewed set.
func expandOn(r catalog.Relation, plan *batchPlan, rows map[string]datum.Row) (int, []batchChange, string, error) {
	keys := datum.Keys(rows)
	matched := 0
	changes := make([]batchChange, 0)
	hasher := sha256.New()
	encoder := json.NewEncoder(hasher)
	for _, key := range keys {
		before := rows[key]
		if !query.Matches(before, plan.Where) {
			continue
		}
		matched++
		change := batchChange{Before: before}
		if !plan.Delete {
			after := datum.Clone(before)
			for column, value := range plan.Set {
				after[column] = value
			}
			normalized, err := r.Normalize(after)
			if err != nil {
				return 0, nil, "", invalid(err)
			}
			if r.Key(normalized) != key {
				return 0, nil, "", fault("invalid", "batch adjustment cannot change the primary key")
			}
			if sameRow(r, before, normalized) {
				continue
			}
			change.After = normalized
			change.Mutation = versions.Mutation{Relation: r.Name, Key: key, Row: normalized}
		} else {
			change.Mutation = versions.Mutation{Relation: r.Name, Key: key, Deleted: true}
		}
		changes = append(changes, change)
		record := struct {
			Key     string    `json:"key"`
			Before  datum.Row `json:"before,omitempty"`
			After   datum.Row `json:"after,omitempty"`
			Deleted bool      `json:"deleted,omitempty"`
		}{Key: key, Before: change.Before, After: change.After, Deleted: change.Mutation.Deleted}
		if err := encoder.Encode(record); err != nil {
			return 0, nil, "", err
		}
	}
	return matched, changes, hex.EncodeToString(hasher.Sum(nil)), nil
}

func sameRow(r catalog.Relation, a, b datum.Row) bool {
	for _, column := range r.Columns {
		if a[column.Name] != b[column.Name] {
			return false
		}
	}
	return true
}

func applyMutation(rows map[string]datum.Row, mutation versions.Mutation) {
	if mutation.Deleted {
		delete(rows, mutation.Key)
	} else {
		rows[mutation.Key] = datum.Clone(mutation.Row)
	}
}

// relationSignature covers exactly the schema and indexes that can constrain
// this relation. A relevant index created after a preview changes this value
// and makes the preview token stale; unrelated catalog maintenance does not.
func (d *DB) relationSignature(r catalog.Relation) string {
	indexes := make([]catalog.Index, 0)
	for _, definition := range d.catalog.IndexList() {
		if definition.Relation == r.Name {
			indexes = append(indexes, definition)
		}
	}
	body, _ := json.Marshal(struct {
		Primary string           `json:"primary"`
		Columns []catalog.Column `json:"columns"`
		Indexes []catalog.Index  `json:"indexes"`
	}{r.Primary, r.Columns, indexes})
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func (d *DB) uniqueIndexes(relation string) []string {
	out := []string{}
	for _, definition := range d.catalog.IndexList() {
		if definition.Relation == relation && definition.Unique {
			out = append(out, definition.Name)
		}
	}
	return out
}

// batchToken binds a preview to the transaction, the exact predicate and set,
// the previewed selection, and the catalog state that constrains it. It
// deliberately excludes sequence-derived frame sizes so an unrelated commit
// elsewhere does not make a valid preview stale.
func batchToken(tx *Transaction, r catalog.Relation, req BatchRequest, signature, digest string, matched, changes int) string {
	body, _ := json.Marshal(struct {
		Transaction string            `json:"transaction"`
		Snapshot    uint64            `json:"snapshot"`
		Relation    string            `json:"relation"`
		Primary     string            `json:"primary"`
		Where       []query.Predicate `json:"where"`
		Set         datum.Row         `json:"set"`
		Delete      bool              `json:"delete"`
		Selection   string            `json:"selection"`
		Matched     int               `json:"matched"`
		Changes     int               `json:"changes"`
		Catalog     string            `json:"catalog"`
	}{tx.ID, tx.Snapshot, r.Name, r.Primary, req.Where, req.Set, req.Delete, digest, matched, changes, signature})
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

// txView builds the transaction-private view of one relation: committed state
// at the snapshot, then each staged operation in stage order. Batch plans see
// the view produced by earlier operations, so explicit single-row edits and
// filtered adjustments compose predictably.
func (d *DB) txView(tx *Transaction, relation string) map[string]datum.Row {
	rows := d.store.Scan(relation, tx.Snapshot)
	for index := range tx.ops {
		op := &tx.ops[index]
		if op.single != nil {
			if op.single.Relation == relation {
				applyMutation(rows, *op.single)
			}
			continue
		}
		if op.batch == nil || op.batch.Relation != relation {
			continue
		}
		_, changes, _, err := expandOn(op.batch.relation(), op.batch, rows)
		if err != nil {
			// Staged plans were valid against their input. A later single-row
			// write can only violate normalization if it set a column to a
			// value that Put already rejected, so this is unreachable in
			// practice; commit still revalidates every resulting row.
			continue
		}
		for _, change := range changes {
			applyMutation(rows, change.Mutation)
		}
	}
	return rows
}

func (d *DB) txTouches(tx *Transaction, relation string) bool {
	for index := range tx.ops {
		op := &tx.ops[index]
		if op.single != nil && op.single.Relation == relation {
			return true
		}
		if op.batch != nil && op.batch.Relation == relation {
			return true
		}
	}
	return false
}

// txWrites reduces all staged operations, plus an optional candidate plan
// applied last, to the final mutation per (relation, key). This is the exact
// write set a commit would publish.
func (d *DB) txWrites(tx *Transaction, extra *batchPlan) (map[string]map[string]versions.Mutation, error) {
	views := map[string]map[string]datum.Row{}
	view := func(relation string) map[string]datum.Row {
		rows, ok := views[relation]
		if !ok {
			rows = d.store.Scan(relation, tx.Snapshot)
			views[relation] = rows
		}
		return rows
	}
	record := func(mutation versions.Mutation, mutations map[string]map[string]versions.Mutation) {
		if mutations[mutation.Relation] == nil {
			mutations[mutation.Relation] = map[string]versions.Mutation{}
		}
		mutations[mutation.Relation][mutation.Key] = mutation
	}
	mutations := map[string]map[string]versions.Mutation{}
	applyPlan := func(plan *batchPlan) error {
		rows := view(plan.Relation)
		_, changes, _, err := expandOn(plan.relation(), plan, rows)
		if err != nil {
			return err
		}
		for _, change := range changes {
			applyMutation(rows, change.Mutation)
			record(change.Mutation, mutations)
		}
		return nil
	}
	for index := range tx.ops {
		op := &tx.ops[index]
		if op.single != nil {
			applyMutation(view(op.single.Relation), *op.single)
			record(*op.single, mutations)
			continue
		}
		if err := applyPlan(op.batch); err != nil {
			return nil, err
		}
	}
	if extra != nil {
		if err := applyPlan(extra); err != nil {
			return nil, err
		}
	}
	return mutations, nil
}

func flatten(mutations map[string]map[string]versions.Mutation) []versions.Mutation {
	out := make([]versions.Mutation, 0)
	for _, perKey := range mutations {
		for _, mutation := range perKey {
			out = append(out, mutation)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Relation == out[j].Relation {
			return out[i].Key < out[j].Key
		}
		return out[i].Relation < out[j].Relation
	})
	return out
}

// frameSize mirrors the exact journal framing for a commit record.
func (d *DB) frameSize(tx *Transaction, writes []versions.Mutation) int {
	record := journal.Record{Format: 1, Sequence: d.sequence + 1, Kind: "commit", Transaction: tx.ID, Writes: writes}
	base, sizes, err := journal.WireSizes(record)
	if err != nil {
		return journal.MaxFrame + 1
	}
	return journal.TotalSize(base, sizes)
}

// assess computes the projected cost and current conflicts of the transaction
// with one more plan appended. It never mutates staged state.
func (d *DB) assess(tx *Transaction, plan *batchPlan, changes int) (BatchCapacity, []BatchConflict, error) {
	mutations, err := d.txWrites(tx, plan)
	if err != nil {
		return BatchCapacity{}, nil, err
	}
	capacity := d.capacityFor(tx, flatten(mutations))
	capacity.Changes = changes
	conflicts := d.batchConflicts(tx, mutations)
	return capacity, conflicts, nil
}

func (d *DB) batchConflicts(tx *Transaction, mutations map[string]map[string]versions.Mutation) []BatchConflict {
	conflicts := []BatchConflict{}
	omitted := 0
	add := func(conflict BatchConflict) {
		if len(conflicts) >= maxPreviewConflicts {
			omitted++
			return
		}
		conflicts = append(conflicts, conflict)
	}
	for _, perKey := range mutations {
		for key, mutation := range perKey {
			if d.store.Latest(mutation.Relation, key) > tx.Snapshot {
				add(BatchConflict{Kind: "write", Key: key, Message: "key changed after the transaction snapshot; commit will conflict"})
			}
		}
	}
	for _, definition := range d.catalog.IndexList() {
		if !definition.Unique {
			continue
		}
		changed := mutations[definition.Relation]
		if len(changed) == 0 {
			continue
		}
		rows := d.store.Scan(definition.Relation, d.sequence)
		for _, mutation := range changed {
			applyMutation(rows, mutation)
		}
		seen := map[string]string{}
		for _, key := range datum.Keys(rows) {
			value := rows[key][definition.Column]
			if value.Null() {
				continue
			}
			token := value.Key()
			if prior, exists := seen[token]; exists && prior != key {
				add(BatchConflict{Kind: "unique", Key: key, Index: definition.Name, Message: "unique constraint " + definition.Name + " would be violated"})
				break
			}
			seen[token] = key
		}
	}
	if omitted > 0 {
		conflicts = append(conflicts, BatchConflict{Kind: "truncated", Message: "additional conflicts omitted"})
	}
	sort.SliceStable(conflicts, func(i, j int) bool {
		if conflicts[i].Kind == conflicts[j].Kind {
			return conflicts[i].Key < conflicts[j].Key
		}
		return conflicts[i].Kind < conflicts[j].Kind
	})
	return conflicts
}

// Batch previews or stages a filtered adjustment. An empty Preview field means
// preview only: nothing is staged and the returned token can be supplied to a
// later call to stage exactly the previewed selection. A matching token stages
// the plan, which then participates in reads and the final commit.
func (d *DB) Batch(id string, req BatchRequest) (BatchPreview, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return BatchPreview{}, err
	}
	tx, err := d.tx(id)
	if err != nil {
		return BatchPreview{}, err
	}
	r, err := d.batchRelation(tx, req.Relation)
	if err != nil {
		return BatchPreview{}, err
	}
	plan, err := validateBatchRequest(r, req)
	if err != nil {
		return BatchPreview{}, err
	}
	limit := req.Limit
	if limit == 0 {
		limit = DefaultBatchPreviewRows
	}
	// Preview expands the complete selection once: capacity and unique
	// conflict preflight need the exact projected write set, so memory is
	// O(scanned rows), the same order as the scan that produced them. Only the
	// echoed rows are capped by limit.
	rows := d.txView(tx, r.Name)
	matched, changes, digest, err := expandOn(r, &plan, rows)
	if err != nil {
		return BatchPreview{}, err
	}
	signature := d.relationSignature(r)
	capacity, conflicts, err := d.assess(tx, &plan, len(changes))
	if err != nil {
		return BatchPreview{}, err
	}
	total := len(changes)
	token := batchToken(tx, r, req, signature, digest, matched, total)
	preview := BatchPreview{
		Relation:         r.Name,
		Snapshot:         tx.Snapshot,
		Head:             d.sequence,
		Matched:          matched,
		Changes:          total,
		Rows:             []BatchRow{},
		Token:            token,
		Feasible:         capacity.OK && len(conflicts) == 0,
		Conflicts:        conflicts,
		Capacity:         capacity,
		UniqueIndexes:    d.uniqueIndexes(r.Name),
		CatalogSignature: signature,
		Note:             "preview is not a reservation; commit revalidates unique indexes against the catalog at publish time",
	}
	for index, change := range changes {
		if index >= limit {
			preview.Truncated = true
			break
		}
		preview.Rows = append(preview.Rows, BatchRow{Key: change.Mutation.Key, Before: change.Before, After: change.After, Deleted: change.Mutation.Deleted})
	}
	if req.Preview == "" {
		return preview, nil
	}
	if tx.ReadOnly {
		return BatchPreview{}, fault("conflict", "read-only transaction")
	}
	if req.Preview != token {
		return BatchPreview{}, &Error{Code: "conflict", Message: "preview token is stale; re-preview required", Details: map[string]any{"reason": "preview_stale"}}
	}
	if !capacity.OK {
		return BatchPreview{}, capacityError(capacity)
	}
	plan.Digest = digest
	plan.Matched = matched
	plan.Changes = len(changes)
	candidate := make([]transactionOp, len(tx.ops), len(tx.ops)+1)
	copy(candidate, tx.ops)
	candidate = append(candidate, transactionOp{batch: &plan})
	if err = d.stage(tx, candidate); err != nil {
		return BatchPreview{}, err
	}
	preview.Staged = true
	preview.Rows = []BatchRow{}
	return preview, nil
}

func capacityError(capacity BatchCapacity) error {
	return &Error{
		Code:    "capacity",
		Message: "transaction write set exceeds a budget; narrow the filter or adjust fewer rows",
		Details: map[string]any{
			"reason":              capacity.Reason,
			"changes":             capacity.Changes,
			"write_set_used":      capacity.WriteSetUsed,
			"write_set_limit":     capacity.WriteSetLimit,
			"projected_mutations": capacity.ProjectedMutations,
			"journal_bytes":       capacity.JournalBytes,
			"journal_limit":       capacity.JournalLimit,
			"atomic":              true,
		},
	}
}
