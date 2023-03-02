package pinyin

import (
	"fmt"
	"testing"
)

var (
	adp = NewAdapter()
)

func TestNewAdapter(t *testing.T) {
	adp := NewAdapter()
	py := adp.ParseHans("朋友你好haha", "", InitialBigLetter)
	fmt.Println(py)
}

func BenchmarkNewAdapter(b *testing.B) {
	for i := 0; i < b.N; i++ {
		adp.ParseHans("好好好", "", InitialBigLetter)
	}
}
