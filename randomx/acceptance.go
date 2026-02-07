package randomx

import "math/rand"

// AcceptanceSampler uses stochastic acceptance sampling.
type AcceptanceSampler[T any] struct {
	items []Item[T]
	max   float64
	rng   *rand.Rand
}

// NewAcceptanceSampler builds an AcceptanceSampler.
func NewAcceptanceSampler[T any](items []Item[T], opts ...Option) (*AcceptanceSampler[T], error) {
	_, max, err := validateItems(items)
	if err != nil {
		return nil, err
	}
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	return &AcceptanceSampler[T]{
		items: append([]Item[T](nil), items...),
		max:   max,
		rng:   cfg.rand,
	}, nil
}

// Sample returns one value following the weights.
func (s *AcceptanceSampler[T]) Sample() (T, bool) {
	var zero T
	if len(s.items) == 0 || s.max == 0 {
		return zero, false
	}
	for {
		idx := s.rng.Intn(len(s.items))
		item := s.items[idx]
		if s.rng.Float64()*s.max <= item.Weight {
			return item.Value, true
		}
	}
}

// SampleN returns n sampled values.
func (s *AcceptanceSampler[T]) SampleN(n int) []T {
	result := make([]T, 0, n)
	for i := 0; i < n; i++ {
		if value, ok := s.Sample(); ok {
			result = append(result, value)
		}
	}
	return result
}
