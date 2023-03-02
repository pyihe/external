package timeheap

import (
	"time"

	"github.com/pyihe/external/internal/fourheap"
	"github.com/pyihe/external/time/internal"
)

type bucket []*internal.Task

func newBucket(c int) *bucket {
	b := make(bucket, 0, c)
	return &b
}

func (b *bucket) Len() int {
	return len(*b)
}

func (b *bucket) Less(i, j int) bool {
	iTime := (*b)[i].Extra[0].(time.Time)
	jTime := (*b)[j].Extra[0].(time.Time)
	return iTime.Before(jTime)
}

func (b *bucket) Swap(i, j int) {
	// 交换元素
	(*b)[i], (*b)[j] = (*b)[j], (*b)[i]

	// 交换索引
	iIndex := (*b)[i].Extra[1].(int)
	jIndex := (*b)[j].Extra[1].(int)
	(*b)[i].Extra[1] = jIndex
	(*b)[j].Extra[1] = iIndex
}

func (b *bucket) Push(x interface{}) {
	t, ok := x.(*internal.Task)
	if !ok {
		return
	}
	n := len(*b)
	c := cap(*b)
	// 需要扩容
	if n+1 > c {
		nb := make(bucket, n, c*2)
		copy(nb, *b)
		*b = nb
	}
	*b = (*b)[0 : n+1]
	(*b)[n] = t
	t.Extra[1] = n
}

func (b *bucket) Pop() interface{} {
	n := len(*b)
	c := cap(*b)
	if n < (c/2) && c > 25 {
		nb := make(bucket, n, c/2)
		copy(nb, *b)
		*b = nb
	}
	if n == 0 {
		return nil
	}
	x := (*b)[n-1]
	(*b)[n-1] = nil
	*b = (*b)[:n-1]
	return x
}

func (b *bucket) peek() *internal.Task {
	if len(*b) == 0 {
		return nil
	}

	return (*b)[0]
}

func (b *bucket) fix(t *internal.Task) {
	index := t.Extra[1].(int)
	fourheap.Fix(b, index)
}

func (b *bucket) delete(i int) {
	fourheap.Remove(b, i)
}

func (b *bucket) add(t *internal.Task) {
	fourheap.Push(b, t)
}
