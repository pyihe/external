package alias

import "testing"

type event struct {
	id   int
	prob float64
}

func (e *event) Id() int {
	return e.id
}

func (e *event) Prob() float64 {
	return e.prob
}

var testdata = []Event{
	&event{1, 0.1},
	&event{2, 0.1},
	&event{3, 0.2},
	&event{4, 0.3},
	&event{5, 0.29},
	&event{6, 0.01},
}

func TestCore_Pick(t *testing.T) {
	s := New()
	if err := s.Add(testdata...); err != nil {
		t.Fatalf("%v\n", err)
	}

	pick(t, s)

	if err := s.Remove(testdata[5]); err != nil {
		t.Fatalf("remove err: %v", err)
	}
	pick(t, s)
}

func pick(t *testing.T, s Sampler) {
	_, result := s.PickN(10000)
	// result := make(map[int]int)
	// for i := 1; i <= 10000; i++ {
	// 	ok, id := s.Pick()
	// 	if !ok {
	// 		continue
	// 	}
	// 	result[id] += 1
	// }
	t.Logf("采样结果为: %v\n", result)
}
