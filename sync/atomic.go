package gosync

import "sync/atomic"

type AtomicInt32 int32

func (i32 *AtomicInt32) Incr(delta int32) {
	atomic.AddInt32((*int32)(i32), delta)
}

func (i32 *AtomicInt32) Load() int32 {
	return atomic.LoadInt32((*int32)(i32))
}

type AtomicInt64 int64

func (i64 *AtomicInt64) Incr(delta int64) {
	atomic.AddInt64((*int64)(i64), delta)
}

func (i64 *AtomicInt64) Load() int64 {
	return atomic.LoadInt64((*int64)(i64))
}
