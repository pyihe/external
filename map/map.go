package gomap

type Map[K comparable, V any] struct {
	mp map[K]V
}

func NewMap[K comparable, V any]() *Map[K, V] {
	return &Map[K, V]{}
}

func (m *Map[K, V]) init() {
	if m.mp == nil {
		m.mp = make(map[K]V)
	}
}

func (m *Map[K, V]) Set(key K, value V) {
	m.init()
	m.mp[key] = value
}

func (m *Map[K, V]) SetNX(key K, value V) {
	m.init()
	if _, exist := m.mp[key]; !exist {
		m.mp[key] = value
	}
}

func (m *Map[K, V]) Get(key K) (value V, exist bool) {
	m.init()
	value, exist = m.mp[key]
	return
}

func (m *Map[K, V]) GetSet(key K, value V) (oldValue V, exist bool) {
	m.init()
	oldValue, exist = m.mp[key]
	m.mp[key] = value
	return
}

func (m *Map[K, V]) Del(key K) {
	delete(m.mp, key)
}

func (m *Map[K, V]) Len() int {
	return len(m.mp)
}

func (m *Map[K, V]) Range(f func(key K, value V) bool) {
	if m.mp != nil {
		for k, v := range m.mp {
			if !f(k, v) {
				break
			}
		}
	}
}
