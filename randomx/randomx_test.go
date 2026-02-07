package randomx

import (
	"math/rand"
	"testing"
)

type testItem struct {
	Name string
}

func TestValidateItemsErrors(t *testing.T) {
	_, _, err := validateItems[int](nil)
	if err != ErrEmptyItems {
		t.Fatalf("expected ErrEmptyItems, got %v", err)
	}

	_, _, err = validateItems([]Item[int]{{Value: 1, Weight: -1}})
	if err != ErrInvalidWeight {
		t.Fatalf("expected ErrInvalidWeight, got %v", err)
	}

	_, _, err = validateItems([]Item[int]{{Value: 1, Weight: 0}})
	if err != ErrTotalWeightZero {
		t.Fatalf("expected ErrTotalWeightZero, got %v", err)
	}
}

func TestPrefixSamplerDeterministic(t *testing.T) {
	items := []Item[testItem]{
		{Value: testItem{Name: "A"}, Weight: 1},
		{Value: testItem{Name: "B"}, Weight: 3},
		{Value: testItem{Name: "C"}, Weight: 6},
	}

	s1, err := NewPrefixSampler(items, WithRand(rand.New(rand.NewSource(1))))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	s2, err := NewPrefixSampler(items, WithRand(rand.New(rand.NewSource(1))))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for i := 0; i < 20; i++ {
		v1, ok1 := s1.Sample()
		v2, ok2 := s2.Sample()
		if !ok1 || !ok2 {
			t.Fatalf("expected samples, got ok1=%v ok2=%v", ok1, ok2)
		}
		if v1 != v2 {
			t.Fatalf("expected deterministic sample at %d, got %v vs %v", i, v1, v2)
		}
	}

	counts := map[string]int{}
	for i := 0; i < 10000; i++ {
		value, ok := s1.Sample()
		if !ok {
			t.Fatal("expected sample")
		}
		counts[value.Name]++
	}
	if !(counts["C"] > counts["B"] && counts["B"] > counts["A"]) {
		t.Fatalf("unexpected distribution: %#v", counts)
	}
}

func TestAliasSamplerDeterministic(t *testing.T) {
	items := []Item[testItem]{
		{Value: testItem{Name: "A"}, Weight: 1},
		{Value: testItem{Name: "B"}, Weight: 3},
		{Value: testItem{Name: "C"}, Weight: 6},
	}

	s1, err := NewAliasSampler(items, WithRand(rand.New(rand.NewSource(1))))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	s2, err := NewAliasSampler(items, WithRand(rand.New(rand.NewSource(1))))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for i := 0; i < 20; i++ {
		v1, ok1 := s1.Sample()
		v2, ok2 := s2.Sample()
		if !ok1 || !ok2 {
			t.Fatalf("expected samples, got ok1=%v ok2=%v", ok1, ok2)
		}
		if v1 != v2 {
			t.Fatalf("expected deterministic sample at %d, got %v vs %v", i, v1, v2)
		}
	}

	counts := map[string]int{}
	for i := 0; i < 10000; i++ {
		value, ok := s1.Sample()
		if !ok {
			t.Fatal("expected sample")
		}
		counts[value.Name]++
	}
	if !(counts["C"] > counts["B"] && counts["B"] > counts["A"]) {
		t.Fatalf("unexpected distribution: %#v", counts)
	}
}

func TestAcceptanceSamplerDeterministic(t *testing.T) {
	items := []Item[testItem]{
		{Value: testItem{Name: "A"}, Weight: 1},
		{Value: testItem{Name: "B"}, Weight: 3},
		{Value: testItem{Name: "C"}, Weight: 6},
	}

	s1, err := NewAcceptanceSampler(items, WithRand(rand.New(rand.NewSource(1))))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	s2, err := NewAcceptanceSampler(items, WithRand(rand.New(rand.NewSource(1))))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for i := 0; i < 20; i++ {
		v1, ok1 := s1.Sample()
		v2, ok2 := s2.Sample()
		if !ok1 || !ok2 {
			t.Fatalf("expected samples, got ok1=%v ok2=%v", ok1, ok2)
		}
		if v1 != v2 {
			t.Fatalf("expected deterministic sample at %d, got %v vs %v", i, v1, v2)
		}
	}

	counts := map[string]int{}
	for i := 0; i < 10000; i++ {
		value, ok := s1.Sample()
		if !ok {
			t.Fatal("expected sample")
		}
		counts[value.Name]++
	}
	if !(counts["C"] > counts["B"] && counts["B"] > counts["A"]) {
		t.Fatalf("unexpected distribution: %#v", counts)
	}
}
