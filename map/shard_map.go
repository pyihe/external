package gomap

/*
	copy from https://github.com/orcaman/concurrent-map
*/

import (
	"encoding/json"
	"fmt"
	"sync"
)

const defaultShardSize = 32

type (
	// UpsertFunc 更新或者插入新key的func
	// exist: 表示需要更新或者插入的key是否存在
	// oldValue: 如果key存在，表示其既有的值
	// updateValue: 需要更新（或插入）的新值
	UpsertFunc[V any] func(exist bool, oldValue V, updateValue V) V

	// DelFunc 删除key执行的func，返回true证明删除成功
	// key: 需要删除的key
	// v: 被删除key对应的value
	// exist: key是否存在
	DelFunc[K any, V any] func(key K, v V, exist bool) bool

	// RangeFunc map迭代器，返回false则终止遍历
	RangeFunc[K comparable, V any] func(key K, value V) bool
)

type Stringer interface {
	fmt.Stringer
	comparable
}

// ShardMap 线程安全的map[comparable, any]，为了避免锁带来的性能瓶颈，将map进行了分片
type ShardMap[K comparable, V any] struct {
	shards    []*ShardUnit[K, V]
	sharding  func(key K) uint32
	shardSize int
}

// ShardUnit 线程安全的map[string]any
type ShardUnit[K comparable, V any] struct {
	items map[K]V
	sync.RWMutex
}

func create[K comparable, V any](shardSize int, sharding func(key K) uint32) *ShardMap[K, V] {
	m := &ShardMap[K, V]{
		shardSize: shardSize,
		sharding:  sharding,
		shards:    make([]*ShardUnit[K, V], shardSize),
	}
	for i := 0; i < shardSize; i++ {
		m.shards[i] = &ShardUnit[K, V]{items: make(map[K]V)}
	}
	return m
}

func NewShardMap[V any](shardSize int) *ShardMap[string, V] {
	if shardSize <= 0 {
		shardSize = defaultShardSize
	}

	return create[string, V](shardSize, fnv32)
}

func NewShardMapWithStringer[K Stringer, V any](shardSize int) *ShardMap[K, V] {
	return create[K, V](shardSize, strfnv32[K])
}

// GetShard returns shard under given key
func (m *ShardMap[K, V]) GetShard(key K) *ShardUnit[K, V] {
	return m.shards[uint(m.sharding(key))%uint(m.shardSize)]
}

func (m *ShardMap[K, V]) MSet(data map[K]V) {
	for key, value := range data {
		shard := m.GetShard(key)
		shard.Lock()
		shard.items[key] = value
		shard.Unlock()
	}
}

func (m *ShardMap[K, V]) Set(key K, value V) {
	shard := m.GetShard(key)
	shard.Lock()
	shard.items[key] = value
	shard.Unlock()
}

// Upsert 通过UpdateCb更新既有的key或者新插入key
func (m *ShardMap[K, V]) Upsert(key K, value V, fn UpsertFunc[V]) (res V) {
	shard := m.GetShard(key)
	shard.Lock()
	v, ok := shard.items[key]
	res = fn(ok, v, value)
	shard.items[key] = res
	shard.Unlock()
	return res
}

// SetNX 仅当key不存在时才set
func (m *ShardMap[K, V]) SetNX(key K, value V) bool {
	// Get map shard.
	shard := m.GetShard(key)
	shard.Lock()
	_, ok := shard.items[key]
	if !ok {
		shard.items[key] = value
	}
	shard.Unlock()
	return !ok
}

func (m *ShardMap[K, V]) Get(key K) (V, bool) {
	// Get shard
	shard := m.GetShard(key)
	shard.RLock()
	// Get item from shard.
	val, ok := shard.items[key]
	shard.RUnlock()
	return val, ok
}

func (m *ShardMap[K, V]) Len() int {
	count := 0
	for i := 0; i < m.shardSize; i++ {
		shard := m.shards[i]
		shard.RLock()
		count += len(shard.items)
		shard.RUnlock()
	}
	return count
}

func (m *ShardMap[K, V]) Exists(key K) bool {
	// Get shard
	shard := m.GetShard(key)
	shard.RLock()
	// See if element is within shard.
	_, ok := shard.items[key]
	shard.RUnlock()
	return ok
}

func (m *ShardMap[K, V]) Del(key K) {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	delete(shard.items, key)
	shard.Unlock()
}

func (m *ShardMap[K, V]) DelWithFunc(key K, fn DelFunc[K, V]) bool {
	shard := m.GetShard(key)
	shard.Lock()
	v, ok := shard.items[key]
	remove := fn(key, v, ok)
	if remove && ok {
		delete(shard.items, key)
	}
	shard.Unlock()
	return remove
}

// Pop removes an element from the map and returns it
func (m *ShardMap[K, V]) Pop(key K) (v V, exists bool) {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	v, exists = shard.items[key]
	delete(shard.items, key)
	shard.Unlock()
	return v, exists
}

// Tuple Used by the Iter & IterBuffered functions to wrap two variables together over a channel,
type Tuple[K comparable, V any] struct {
	Key K
	Val V
}

// IterBuffered returns a buffered iterator which could be used in a for range loop.
func (m *ShardMap[K, V]) IterBuffered() <-chan Tuple[K, V] {
	chans := snapshot(*m)
	total := 0
	for _, c := range chans {
		total += cap(c)
	}
	ch := make(chan Tuple[K, V], total)
	go fanIn(chans, ch)
	return ch
}

// Clean 清除所有key&value
func (m *ShardMap[K, V]) Clean() {
	for item := range m.IterBuffered() {
		m.Del(item.Key)
	}
}

// Returns a array of channels that contains elements in each shard,
// which likely takes a snapshot of `m`.
// It returns once the size of each buffered channel is determined,
// before all the channels are populated using goroutines.
func snapshot[K comparable, V any](m ShardMap[K, V]) (chans []chan Tuple[K, V]) {
	// When you access map items before initializing.
	if len(m.shards) == 0 {
		panic(`ShardMap is not initialized. Should run New() before usage.`)
	}
	chans = make([]chan Tuple[K, V], m.shardSize)
	wg := sync.WaitGroup{}
	wg.Add(m.shardSize)
	// Foreach shard.
	for index, shard := range m.shards {
		go func(index int, shard *ShardUnit[K, V]) {
			// Foreach key, value pair.
			shard.RLock()
			chans[index] = make(chan Tuple[K, V], len(shard.items))
			wg.Done()
			for key, val := range shard.items {
				chans[index] <- Tuple[K, V]{key, val}
			}
			shard.RUnlock()
			close(chans[index])
		}(index, shard)
	}
	wg.Wait()
	return chans
}

func fanIn[K comparable, V any](chans []chan Tuple[K, V], out chan Tuple[K, V]) {
	wg := sync.WaitGroup{}
	wg.Add(len(chans))
	for _, ch := range chans {
		go func(ch chan Tuple[K, V]) {
			for t := range ch {
				out <- t
			}
			wg.Done()
		}(ch)
	}
	wg.Wait()
	close(out)
}

func (m *ShardMap[K, V]) Range(f RangeFunc[K, V]) {
	for idx := range m.shards {
		shard := m.shards[idx]
		shard.RLock()
		for key, value := range shard.items {
			if !f(key, value) {
				shard.RUnlock()
				return
			}
		}
		shard.RUnlock()
	}
}

func (m *ShardMap[K, V]) Keys() []K {
	count := m.Len()
	ch := make(chan K, count)
	go func() {
		// Foreach shard.
		wg := sync.WaitGroup{}
		wg.Add(m.shardSize)
		for _, shard := range m.shards {
			go func(shard *ShardUnit[K, V]) {
				// Foreach key, value pair.
				shard.RLock()
				for key := range shard.items {
					ch <- key
				}
				shard.RUnlock()
				wg.Done()
			}(shard)
		}
		wg.Wait()
		close(ch)
	}()

	// Generate keys
	keys := make([]K, 0, count)
	for k := range ch {
		keys = append(keys, k)
	}
	return keys
}

// MarshalJSON JSON序列化
func (m *ShardMap[K, V]) MarshalJSON() ([]byte, error) {
	tmp := make(map[K]V)

	for item := range m.IterBuffered() {
		tmp[item.Key] = item.Val
	}
	return json.Marshal(tmp)
}

// UnmarshalJSON JSON反序列化
// 先反序列化进一个map中，然后一次set进ShardMap
func (m *ShardMap[K, V]) UnmarshalJSON(b []byte) (err error) {
	tmp := make(map[K]V)

	if err = json.Unmarshal(b, &tmp); err == nil {
		for key, val := range tmp {
			m.Set(key, val)
		}
	}

	return
}

func strfnv32[K fmt.Stringer](key K) uint32 {
	return fnv32(key.String())
}

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	keyLength := len(key)
	for i := 0; i < keyLength; i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}
