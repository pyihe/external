package randomx

import (
	"errors"
	"math/rand"
	"time"
)

// Number represents numeric types that can be used as weights.
type Number interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr |
		~float32 | ~float64
}

// WeightFunc returns the weight for a given item.
type WeightFunc[T any, W Number] func(T) W

var (
	// ErrEmptyItems indicates the input slice is empty.
	ErrEmptyItems = errors.New("randomx: empty items")
	// ErrInvalidWeights indicates the input weights contain invalid values.
	ErrInvalidWeights = errors.New("randomx: invalid weights")
	// ErrNilWeightFunc indicates the weight function is nil.
	ErrNilWeightFunc = errors.New("randomx: nil weight function")
)

// Choose selects one item using a single-pass weighted random algorithm.
func Choose[T any, W Number](items []T, weight WeightFunc[T, W], rng *rand.Rand) (T, error) {
	var zero T
	if len(items) == 0 {
		return zero, ErrEmptyItems
	}
	if weight == nil {
		return zero, ErrNilWeightFunc
	}

	total := 0.0
	weights := make([]float64, len(items))
	for i, item := range items {
		w := float64(weight(item))
		if w < 0 {
			return zero, ErrInvalidWeights
		}
		weights[i] = w
		total += w
	}
	if total == 0 {
		return zero, ErrInvalidWeights
	}

	target := randomFloat64(rng) * total
	running := 0.0
	for i, w := range weights {
		running += w
		if target < running {
			return items[i], nil
		}
	}
	return items[len(items)-1], nil
}

func randomFloat64(rng *rand.Rand) float64 {
	if rng == nil {
		return rand.New(rand.NewSource(time.Now().UnixNano())).Float64()
	}
	return rng.Float64()
}
