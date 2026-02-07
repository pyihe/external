package randomx

import "math/rand"

// CDFPicker selects items using a precomputed cumulative distribution.
type CDFPicker[T any] struct {
	items      []T
	cumulative []float64
	total      float64
}

// NewCDFPicker builds a picker that uses cumulative distribution sampling.
func NewCDFPicker[T any, W Number](items []T, weight WeightFunc[T, W]) (*CDFPicker[T], error) {
	if len(items) == 0 {
		return nil, ErrEmptyItems
	}
	if weight == nil {
		return nil, ErrNilWeightFunc
	}

	cumulative := make([]float64, len(items))
	total := 0.0
	for i, item := range items {
		w := float64(weight(item))
		if w < 0 {
			return nil, ErrInvalidWeights
		}
		total += w
		cumulative[i] = total
	}
	if total == 0 {
		return nil, ErrInvalidWeights
	}

	return &CDFPicker[T]{
		items:      append([]T(nil), items...),
		cumulative: cumulative,
		total:      total,
	}, nil
}

// Pick returns a random item based on the cumulative distribution.
func (p *CDFPicker[T]) Pick(rng *rand.Rand) (T, error) {
	var zero T
	if p == nil || len(p.items) == 0 || p.total <= 0 {
		return zero, ErrInvalidWeights
	}

	target := randomFloat64(rng) * p.total
	low, high := 0, len(p.cumulative)-1
	for low < high {
		mid := (low + high) / 2
		if target < p.cumulative[mid] {
			high = mid
		} else {
			low = mid + 1
		}
	}
	return p.items[low], nil
}
