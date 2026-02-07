package randomx

import (
	"math/rand"
	"testing"
)

func TestChooseSingleWeight(t *testing.T) {
	items := []string{"a", "b", "c"}
	weights := map[string]int{"a": 0, "b": 5, "c": 0}
	rng := rand.New(rand.NewSource(1))

	for i := 0; i < 10; i++ {
		item, err := Choose(items, func(s string) int { return weights[s] }, rng)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if item != "b" {
			t.Fatalf("expected b, got %s", item)
		}
	}
}

func TestCDFPickerZeroWeightNotSelected(t *testing.T) {
	items := []int{1, 2, 3}
	weights := []int{1, 0, 2}
	rng := rand.New(rand.NewSource(2))

	picker, err := NewCDFPicker(items, func(i int) int { return weights[i-1] })
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for i := 0; i < 50; i++ {
		item, err := picker.Pick(rng)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if item == 2 {
			t.Fatalf("unexpected zero-weight item selected")
		}
	}
}

func TestAliasPickerZeroWeightNotSelected(t *testing.T) {
	items := []int{1, 2, 3}
	weights := []int{2, 0, 1}
	rng := rand.New(rand.NewSource(3))

	picker, err := NewAliasPicker(items, func(i int) int { return weights[i-1] })
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for i := 0; i < 50; i++ {
		item, err := picker.Pick(rng)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if item == 2 {
			t.Fatalf("unexpected zero-weight item selected")
		}
	}
}

func TestInvalidInputs(t *testing.T) {
	if _, err := Choose([]int{}, func(int) int { return 1 }, rand.New(rand.NewSource(1))); err != ErrEmptyItems {
		t.Fatalf("expected ErrEmptyItems, got %v", err)
	}

	if _, err := NewCDFPicker[int, int]([]int{1}, nil); err != ErrNilWeightFunc {
		t.Fatalf("expected ErrNilWeightFunc, got %v", err)
	}

	if _, err := NewAliasPicker([]int{1, 2}, func(i int) int {
		if i == 2 {
			return -1
		}
		return 1
	}); err != ErrInvalidWeights {
		t.Fatalf("expected ErrInvalidWeights, got %v", err)
	}
}
