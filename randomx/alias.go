package randomx

import "math/rand"

// AliasSampler uses the alias method for O(1) sampling.
type AliasSampler[T any] struct {
	items []Item[T]
	prob  []float64
	alias []int
	rng   *rand.Rand
}

// NewAliasSampler builds an AliasSampler.
func NewAliasSampler[T any](items []Item[T], opts ...Option) (*AliasSampler[T], error) {
	total, _, err := validateItems(items)
	if err != nil {
		return nil, err
	}
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	n := len(items)
	prob := make([]float64, n)
	alias := make([]int, n)
	scaled := make([]float64, n)
	for i, item := range items {
		scaled[i] = item.Weight * float64(n) / total
	}

	small := make([]int, 0, n)
	large := make([]int, 0, n)
	for i, weight := range scaled {
		if weight < 1.0 {
			small = append(small, i)
		} else {
			large = append(large, i)
		}
	}

	for len(small) > 0 && len(large) > 0 {
		l := small[len(small)-1]
		small = small[:len(small)-1]
		g := large[len(large)-1]
		large = large[:len(large)-1]

		prob[l] = scaled[l]
		alias[l] = g
		scaled[g] = scaled[g] + scaled[l] - 1.0
		if scaled[g] < 1.0 {
			small = append(small, g)
		} else {
			large = append(large, g)
		}
	}

	for _, idx := range append(small, large...) {
		prob[idx] = 1.0
		alias[idx] = idx
	}

	return &AliasSampler[T]{
		items: append([]Item[T](nil), items...),
		prob:  prob,
		alias: alias,
		rng:   cfg.rand,
	}, nil
}

// Sample returns one value following the weights.
func (s *AliasSampler[T]) Sample() (T, bool) {
	var zero T
	if len(s.items) == 0 {
		return zero, false
	}
	idx := s.rng.Intn(len(s.items))
	if s.rng.Float64() < s.prob[idx] {
		return s.items[idx].Value, true
	}
	aliasIdx := s.alias[idx]
	if aliasIdx < 0 || aliasIdx >= len(s.items) {
		return zero, false
	}
	return s.items[aliasIdx].Value, true
}

// SampleN returns n sampled values.
func (s *AliasSampler[T]) SampleN(n int) []T {
	result := make([]T, 0, n)
	for i := 0; i < n; i++ {
		if value, ok := s.Sample(); ok {
			result = append(result, value)
		}
	}
	return result
}
