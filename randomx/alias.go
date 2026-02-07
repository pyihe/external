package randomx

import "math/rand"

// AliasPicker selects items in O(1) time using the alias method.
type AliasPicker[T any] struct {
	items []T
	prob  []float64
	alias []int
}

// NewAliasPicker builds a picker using the alias method for weighted sampling.
func NewAliasPicker[T any, W Number](items []T, weight WeightFunc[T, W]) (*AliasPicker[T], error) {
	if len(items) == 0 {
		return nil, ErrEmptyItems
	}
	if weight == nil {
		return nil, ErrNilWeightFunc
	}

	n := len(items)
	weights := make([]float64, n)
	total := 0.0
	for i, item := range items {
		w := float64(weight(item))
		if w < 0 {
			return nil, ErrInvalidWeights
		}
		weights[i] = w
		total += w
	}
	if total == 0 {
		return nil, ErrInvalidWeights
	}

	scaled := make([]float64, n)
	for i, w := range weights {
		scaled[i] = w * float64(n) / total
	}

	prob := make([]float64, n)
	alias := make([]int, n)
	small := make([]int, 0, n)
	large := make([]int, 0, n)

	for i, w := range scaled {
		if w < 1.0 {
			small = append(small, i)
		} else {
			large = append(large, i)
		}
	}

	for len(small) > 0 && len(large) > 0 {
		s := small[len(small)-1]
		small = small[:len(small)-1]
		l := large[len(large)-1]
		large = large[:len(large)-1]

		prob[s] = scaled[s]
		alias[s] = l

		scaled[l] = scaled[l] + scaled[s] - 1.0
		if scaled[l] < 1.0 {
			small = append(small, l)
		} else {
			large = append(large, l)
		}
	}

	for _, idx := range append(small, large...) {
		prob[idx] = 1.0
		alias[idx] = idx
	}

	return &AliasPicker[T]{
		items: append([]T(nil), items...),
		prob:  prob,
		alias: alias,
	}, nil
}

// Pick returns a random item using the alias table.
func (p *AliasPicker[T]) Pick(rng *rand.Rand) (T, error) {
	var zero T
	if p == nil || len(p.items) == 0 {
		return zero, ErrInvalidWeights
	}

	n := len(p.items)
	index := int(randomFloat64(rng) * float64(n))
	if index >= n {
		index = n - 1
	}
	if randomFloat64(rng) < p.prob[index] {
		return p.items[index], nil
	}
	return p.items[p.alias[index]], nil
}
