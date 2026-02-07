package randomx

import "errors"

// Item represents a weighted value for sampling.
type Item[T any] struct {
	Value  T
	Weight float64
}

var (
	ErrEmptyItems      = errors.New("no items provided")
	ErrInvalidWeight   = errors.New("invalid weight")
	ErrTotalWeightZero = errors.New("total weight is zero")
)

func validateItems[T any](items []Item[T]) (total float64, max float64, err error) {
	if len(items) == 0 {
		return 0, 0, ErrEmptyItems
	}
	for _, item := range items {
		if item.Weight < 0 {
			return 0, 0, ErrInvalidWeight
		}
		total += item.Weight
		if item.Weight > max {
			max = item.Weight
		}
	}
	if total == 0 {
		return 0, max, ErrTotalWeightZero
	}
	return total, max, nil
}
