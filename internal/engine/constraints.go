package engine

import (
	"lamina/internal/catalog"
	"lamina/internal/datum"
	"lamina/internal/index"
	"lamina/internal/journal"
	"lamina/internal/versions"
)

// uniqueFinal validates every unique index over the final state that a commit
// would publish: committed state at the current sequence plus the complete
// write set. Historical index candidates are never consulted. A unique index
// created while the transaction was staging is therefore enforced at commit
// even though it did not exist at the transaction snapshot.
func (d *DB) uniqueFinal(mutations map[string]map[string]versions.Mutation) error {
	if len(mutations) == 0 {
		return nil
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
				return fault("conflict", "unique constraint %s violated", definition.Name)
			}
			seen[token] = key
		}
	}
	return nil
}

// unique validates the committed state of a relation, optionally overlaid with
// explicit mutations. It is used at startup after replay.
func (d *DB) unique(table string, mutations map[string]map[string]versions.Mutation) error {
	rows := d.store.Scan(table, d.sequence)
	for _, mutation := range mutations[table] {
		applyMutation(rows, mutation)
	}
	for _, i := range d.catalog.Indexes {
		if i.Relation == table && i.Unique {
			if err := checkUnique(rows, i); err != nil {
				return err
			}
		}
	}
	return nil
}
func checkUnique(rows map[string]datum.Row, i catalog.Index) error {
	values := map[string]string{}
	for key, row := range rows {
		v := row[i.Column]
		if v.Null() {
			continue
		}
		token := v.Key()
		if prior, exists := values[token]; exists && prior != key {
			return fault("conflict", "unique constraint %s violated", i.Name)
		}
		values[token] = key
	}
	return nil
}
func (d *DB) CreateRelation(r catalog.Relation) (catalog.Relation, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return r, err
	}
	if err := r.Validate(); err != nil {
		return r, invalid(err)
	}
	if _, exists := d.catalog.Relations[r.Name]; exists {
		return r, fault("conflict", "relation exists")
	}
	r.Created = d.sequence + 1
	if err := d.publish(journal.Record{Kind: "relation", Relation: &r}); err != nil {
		return r, err
	}
	return r, nil
}

// CreateIndex is a durable single operation. It validates the unique constraint
// against the currently committed final state and never consults historical
// index candidates. A failed build publishes nothing.
func (d *DB) CreateIndex(i catalog.Index) (catalog.Index, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return i, err
	}
	if err := d.catalog.CheckIndex(i); err != nil {
		return i, invalid(err)
	}
	if i.Unique {
		if err := checkUnique(d.store.Scan(i.Relation, d.sequence), i); err != nil {
			return i, err
		}
	}
	i.Created = d.sequence + 1
	if err := d.publish(journal.Record{Kind: "index", Index: &i}); err != nil {
		return i, err
	}
	d.indexes = index.Build(d.catalog, d.store)
	return i, nil
}
