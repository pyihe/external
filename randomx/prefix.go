package randomx

import (
	"math/rand"
	"sort"
)

// PrefixSampler uses cumulative weights with binary search.
type PrefixSampler[T any] struct {
	items  []Item[T]
	prefix []float64
	total  float64
	rng    *rand.Rand
}

// NewPrefixSampler builds a PrefixSampler.
func NewPrefixSampler[T any](items []Item[T], opts ...Option) (*PrefixSampler[T], error) {
	total, _, err := validateItems(items)
	if err != nil {
		return nil, err
	}
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	prefix := make([]float64, len(items))
	var running float64
	for i, item := range items {
		running += item.Weight
		prefix[i] = running
	}

	return &PrefixSampler[T]{
		items:  append([]Item[T](nil), items...),
		prefix: prefix,
		total:  total,
		rng:    cfg.rand,
	}, nil
}

// Sample returns one value following the weights.
func (s *PrefixSampler[T]) Sample() (T, bool) {
	var zero T
	if s.total == 0 || len(s.items) == 0 {
		return zero, false
	}
	target := s.rng.Float64() * s.total
	idx := sort.Search(len(s.prefix), func(i int) bool { return s.prefix[i] > target })
	if idx < 0 || idx >= len(s.items) {
		return zero, false
	}
	return s.items[idx].Value, true
}

// SampleN returns n sampled values.
func (s *PrefixSampler[T]) SampleN(n int) []T {
	result := make([]T, 0, n)
	for i := 0; i < n; i++ {
		if value, ok := s.Sample(); ok {
			result = append(result, value)
		}
	}
	return result
}
