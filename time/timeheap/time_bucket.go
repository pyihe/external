package timeheap

import (
	"context"
	"math"
	"sync"
	"time"

	gomap "github.com/pyihe/external/map"
	"github.com/pyihe/external/time/internal"
)

type timeBucket struct {
	mu sync.RWMutex // guard below
	b  *bucket      // 任务列表

	taskMap       *gomap.LockMap[uint64, *internal.Task] // 保存任务的map， 用于快速查找
	recycleNotify chan *internal.Task                    // 回收任务
	ticker        *time.Ticker                           // 定时器
	heapNotify    chan struct{}                          // 重新堆化时需要通知重新获取堆顶元素
	asynExec      asynHandler                            // 执行任务的handler
}

func newTimeBucket(recycleChan chan *internal.Task, exec asynHandler) *timeBucket {
	tb := &timeBucket{
		mu:            sync.RWMutex{},
		b:             newBucket(bucketLen), // bucket初始容量设置为64
		taskMap:       gomap.NewLockMap[uint64, *internal.Task](),
		heapNotify:    make(chan struct{}, 1),
		asynExec:      exec,
		recycleNotify: recycleChan,
	}
	return tb
}

func (tb *timeBucket) start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			if tb.ticker != nil {
				tb.ticker.Stop()
			}
			return
		default:
			tb.tick()
		}
	}
}

func (tb *timeBucket) tick() {
	const maxDuration = time.Duration(math.MaxInt32) * time.Second

	var duration time.Duration

	tb.mu.RLock()
	t := tb.b.peek()
	tb.mu.RUnlock()

	duration = maxDuration
	if t != nil {
		deadline := t.Extra[0].(time.Time)
		duration = deadline.Sub(time.Now())
	}
	// 高并发情况下，定时器被 heapNotify 中断，再次执行时，任务已经过期，这时duration为负数，
	// 当duration为负数时不应依赖于ticker，直接执行任务
	if duration <= 0 {
		tb.runTask(t)
		return
	}
	if tb.ticker == nil {
		tb.ticker = time.NewTicker(duration)
	} else {
		tb.ticker.Reset(duration)
	}
	select {
	case <-tb.heapNotify:
		tb.ticker.Stop()
		break
	case <-tb.ticker.C:
		tb.runTask(t)
	}
}

func (tb *timeBucket) runTask(t *internal.Task) {
	// 执行任务
	job := t.Job
	tb.asynExec(job)

	index := t.Extra[1].(int)
	// 删除任务
	tb.mu.Lock()
	tb.b.delete(index)
	tb.mu.Unlock()

	// 如果是重复执行，则再次添加
	switch {
	case t.Repeated: // 重复执行的任务
		tb.add(t)
	case t.Expr != nil: // Cron类型的任务
		now := time.Now()
		nextTime := t.Expr.Next(now)
		switch nextTime.IsZero() {
		case true:
			tb.taskMap.Del(t.ID)
			tb.asynExec(func() {
				tb.recycleNotify <- t
			})
		default:
			t.Delay = nextTime.Sub(now)
			tb.add(t)
		}
	default: // 单次执行的任务
		// 否则从任务列表中删除
		tb.taskMap.Del(t.ID)
		// 同时回收任务变量
		tb.asynExec(func() {
			tb.recycleNotify <- t
		})
	}
}

func (tb *timeBucket) add(t *internal.Task) {
	// 更新截止时间
	t.Extra[0] = time.Now().Add(t.Delay)

	tb.mu.Lock()
	tb.b.add(t)
	tb.mu.Unlock()

	tb.taskMap.Set(t.ID, t)

	// 通知tick，有新的任务来了，需要重新找延时最少的任务
	// 防止阻塞，异步通知
	tb.asynExec(func() {
		tb.heapNotify <- struct{}{}
	})
}

func (tb *timeBucket) delete(id uint64) {
	t, exist := tb.taskMap.Get(id)
	if !exist {
		return
	}

	tb.taskMap.Del(id)

	tb.mu.Lock()
	tb.b.delete(t.Extra[1].(int))
	tb.mu.Unlock()

	// 防止阻塞，异步通知
	tb.asynExec(func() {
		tb.heapNotify <- struct{}{}
	})
}
