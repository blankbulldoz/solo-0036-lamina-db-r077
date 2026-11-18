package journal

import (
	"encoding/json"
	"lamina/internal/catalog"
	"lamina/internal/versions"
)

const MaxFrame = 16 << 20

// WritesFieldOverhead is the JSON prefix `,"writes":[` before the elements.
const WritesFieldOverhead = len(`,"writes":[`)

type Record struct {
	Format      int                 `json:"format"`
	Sequence    uint64              `json:"sequence"`
	Kind        string              `json:"kind"`
	Transaction string              `json:"transaction,omitempty"`
	Relation    *catalog.Relation   `json:"relation,omitempty"`
	Index       *catalog.Index      `json:"index,omitempty"`
	Writes      []versions.Mutation `json:"writes,omitempty"`
	Horizon     uint64              `json:"horizon,omitempty"`
}

// WireSizes returns the exact frame payload size of a record without allocating
// the combined payload. base is the size of the record with no writes; sizes
// holds one encoded size per mutation. TotalSize combines them with the same
// length as json.Marshal produces for a non-empty writes array (field order is
// irrelevant to JSON length). Capacity admission and the durable writer thus
// share one framing rule.
func WireSizes(r Record) (base int, sizes []int, err error) {
	writes := r.Writes
	r.Writes = nil
	empty, err := json.Marshal(r)
	if err != nil {
		return 0, nil, err
	}
	base = len(empty)
	sizes = make([]int, 0, len(writes))
	for _, mutation := range writes {
		encoded, err := json.Marshal(mutation)
		if err != nil {
			return 0, nil, err
		}
		sizes = append(sizes, len(encoded))
	}
	return base, sizes, nil
}

// TotalSize matches json.Marshal for a record that carries writes:
// one array prefix, one comma between elements, plus the closing bracket.
func TotalSize(base int, sizes []int) int {
	if len(sizes) == 0 {
		return base
	}
	total := base + WritesFieldOverhead + len(sizes)
	for _, size := range sizes {
		total += size
	}
	return total
}

func PayloadSizeWithin(r Record, limit int) (size int, within bool, err error) {
	base, sizes, err := WireSizes(r)
	if err != nil {
		return 0, false, err
	}
	total := TotalSize(base, sizes)
	return total, total <= limit, nil
}
