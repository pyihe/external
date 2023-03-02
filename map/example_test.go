package gomap

import (
	"strconv"
	"testing"
)

var (
	m  = NewMap[string, int]()
	lm = NewLockMap[string, int]()
	sm = NewShardMap[int](0)
)

func BenchmarkMap_Set(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := strconv.Itoa(i)
		m.Set(key, i)
		m.Get(key)
		m.Del(key)
	}
}

func BenchmarkLockMap_Set(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := strconv.Itoa(i)
		lm.Set(key, i)
		lm.Get(key)
		lm.Del(key)
	}
}

func BenchmarkShardMap_Set(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := strconv.Itoa(i)
		sm.Set(key, i)
		sm.Get(key)
		sm.Del(key)
	}
}
