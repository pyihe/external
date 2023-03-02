package timeheap

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pyihe/external/concurrency"
	"github.com/pyihe/external/cronexpr"
	gotime "github.com/pyihe/external/time"
	"github.com/pyihe/external/time/internal"
)

const (
	bucketLen = 64

	concurrencyKey = "heapTimer"
)

type asynHandler func(func())

type heapTimer struct {
	timeBuckets [bucketLen]*timeBucket // bucket
	recycleChan chan *internal.Task    // 用于回收task变量的通道
	cancel      context.CancelFunc     // 停止所有协程（不包括执行任务的协程）
	bufferChan  chan interface{}       // 用于添加、删除任务的通道
	taskMap     *sync.Map              // key: task.id	value: bucket index
	closed      int32                  // timer是否关闭
	bucketPos   int                    // bucket索引
}

func New(bufferSize int) gotime.Timer {
	var ctx context.Context
	var h = &heapTimer{
		taskMap:     &sync.Map{},
		timeBuckets: [64]*timeBucket{},
		recycleChan: make(chan *internal.Task, bucketLen),
		closed:      0,
		bucketPos:   -1,
		bufferChan:  make(chan interface{}, bufferSize),
	}
	ctx, h.cancel = context.WithCancel(context.Background())

	h.init()
	h.start(ctx)
	return h
}

func (h *heapTimer) init() {
	for i := range h.timeBuckets {
		h.timeBuckets[i] = newTimeBucket(h.recycleChan, h.exec)
	}
}

func (h *heapTimer) start(ctx context.Context) {
	// 开启每个桶的任务监控协程
	for _, tb := range h.timeBuckets {
		tb := tb
		concurrency.Go(concurrencyKey, func() {
			tb.start(ctx)
		})
	}

	concurrency.Go(concurrencyKey, func() {
		h.run(ctx)
	})

	concurrency.Go(concurrencyKey, func() {
		h.recycle(ctx)
	})
}

func (h *heapTimer) recycle(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case t := <-h.recycleChan:
			if t != nil {
				h.taskMap.Delete(t.ID)
				internal.Put(t)
			}
		}
	}
}

func (h *heapTimer) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case t := <-h.bufferChan:
			switch t.(type) {
			case *internal.Task:
				h.addTask(t.(*internal.Task))
			case int64:
				h.deleteTask(t.(uint64))
			}
		}
	}
}

func (h *heapTimer) isClosed() bool {
	return atomic.LoadInt32(&h.closed) == 1
}

func (h *heapTimer) exec(fn func()) {
	concurrency.Go(concurrencyKey, fn)
}

func (h *heapTimer) addTask(t *internal.Task) {
	// 轮询的往bucket中添加延时任务
	h.bucketPos = (h.bucketPos + 1) % bucketLen
	bkt := h.timeBuckets[h.bucketPos]
	bkt.add(t)
	h.taskMap.Store(t.ID, h.bucketPos)
}

func (h *heapTimer) deleteTask(taskId uint64) {
	v, exist := h.taskMap.Load(taskId)
	if !exist {
		return
	}
	h.timeBuckets[v.(int)].delete(taskId)
	h.taskMap.Delete(taskId)
}

func (h *heapTimer) After(delay time.Duration, fn func()) (uint64, error) {
	if fn == nil {
		return 0, gotime.ErrNilFunc
	}
	if h.isClosed() {
		return 0, gotime.ErrTimerClosed
	}

	t := internal.Get(delay, fn, false, nil)
	concurrency.Go(concurrencyKey, func() {
		h.bufferChan <- t
	})
	return t.ID, nil
}

func (h *heapTimer) Every(delay time.Duration, fn func()) (uint64, error) {
	if fn == nil {
		return 0, gotime.ErrNilFunc
	}
	if h.isClosed() {
		return 0, gotime.ErrTimerClosed
	}

	t := internal.Get(delay, fn, true, nil)
	concurrency.Go(concurrencyKey, func() {
		h.bufferChan <- t
	})
	return t.ID, nil
}

func (h *heapTimer) Delete(id uint64) error {
	if id == 0 {
		return nil
	}
	if h.isClosed() {
		return gotime.ErrTimerClosed
	}
	concurrency.Go(concurrencyKey, func() {
		h.bufferChan <- id
	})
	return nil
}

func (h *heapTimer) Stop() {
	if !atomic.CompareAndSwapInt32(&h.closed, 0, 1) {
		return
	}
	h.cancel()
	// 释放协程池
	concurrency.Stop(concurrencyKey)

	n := 0
	h.taskMap.Range(func(key, value any) bool {
		n += 1
		return true
	})
}

func (h *heapTimer) Cron(desc string, fn func()) (uint64, error) {
	if fn == nil {
		return 0, gotime.ErrNilFunc
	}
	if h.isClosed() {
		return 0, gotime.ErrTimerClosed
	}
	// 解析desc
	expr, err := cronexpr.Parse(desc)
	if err != nil {
		return 0, err
	}

	var now = time.Now()
	var nextTime = expr.Next(now)
	if nextTime.IsZero() {
		return 0, err
	}
	var t = internal.Get(nextTime.Sub(now), fn, false, expr)
	concurrency.Go(concurrencyKey, func() {
		h.bufferChan <- t
	})
	return t.ID, nil
}
