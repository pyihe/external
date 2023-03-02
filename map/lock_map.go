package gomap

import "sync"

type LockMap[K comparable, V any] struct {
	mu sync.RWMutex
	mp map[K]V
}

func NewLockMap[K comparable, V any]() *LockMap[K, V] {
	return &LockMap[K, V]{
		mu: sync.RWMutex{},
		mp: make(map[K]V),
	}
}

func (m *LockMap[K, V]) init() {
	if m.mp == nil {
		m.mp = make(map[K]V)
	}
}

func (m *LockMap[K, V]) UnsafeSet(key K, value V) {
	m.init()
	m.mp[key] = value
}

func (m *LockMap[K, V]) Set(key K, value V) {
	m.mu.Lock()
	m.UnsafeSet(key, value)
	m.mu.Unlock()
}

func (m *LockMap[K, V]) UnsafeSetNX(key K, value V) {
	m.init()
	if _, exist := m.mp[key]; !exist {
		m.mp[key] = value
	}
}

func (m *LockMap[K, V]) SetNX(key K, value V) {
	m.mu.Lock()
	m.UnsafeSetNX(key, value)
	m.mu.Unlock()
}

func (m *LockMap[K, V]) UnsafeGet(key K) (value V, exist bool) {
	if m.mp != nil {
		value, exist = m.mp[key]
	}
	return
}

func (m *LockMap[K, V]) Get(key K) (value V, exist bool) {
	m.mu.RLock()
	value, exist = m.UnsafeGet(key)
	m.mu.RUnlock()
	return
}

func (m *LockMap[K, V]) UnsafeGetSet(key K, value V) (oldValue V, exist bool) {
	m.init()
	oldValue, exist = m.mp[key]
	m.mp[key] = value
	return
}

func (m *LockMap[K, V]) GetSet(key K, value V) (oldValue V, exist bool) {
	m.mu.Lock()
	oldValue, exist = m.UnsafeGetSet(key, value)
	m.mu.Unlock()
	return
}

func (m *LockMap[K, V]) UnsafeDel(key K) {
	delete(m.mp, key)
}

func (m *LockMap[K, V]) Del(key K) {
	m.mu.Lock()
	m.UnsafeDel(key)
	m.mu.Unlock()
}

func (m *LockMap[K, V]) UnsafeLen() int {
	return len(m.mp)
}

func (m *LockMap[K, V]) Len() (n int) {
	m.mu.RLock()
	n = m.UnsafeLen()
	m.mu.RUnlock()
	return
}

func (m *LockMap[K, V]) SafeRange(f func(key K, value V) bool) {
	m.mu.Lock()
	for k, v := range m.mp {
		if !f(k, v) {
			break
		}
	}
	m.mu.Unlock()
}

func (m *LockMap[K, V]) UnsafeRange(f func(key K, value V) bool) {
	for k, v := range m.mp {
		if !f(k, v) {
			break
		}
	}
}
