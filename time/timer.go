package gotime

import (
	"context"
	"errors"
	"time"
)

var (
	ErrNilFunc     = errors.New("function must not be nil")
	ErrTimerClosed = errors.New("time closed")
	ErrInvalidExpr = errors.New("invalid cron desc")
)

type Timer interface {
	Stop()                                            // 停止定时器
	Delete(uint64) error                              // 删除指定ID的任务
	After(d time.Duration, fn func()) (uint64, error) // duration后执行一次, 返回任务ID
	Every(d time.Duration, fn func()) (uint64, error) // 每duration执行一次, 返回任务ID
	Cron(desc string, fn func()) (uint64, error)      // 按照给定的DESC执行任务
}

// SleepWithCtx 携带ctx的Sleep，可以通过ctx控制提前终止sleep
func SleepWithCtx(ctx context.Context, d time.Duration) (err error) {
	ticker := time.NewTicker(d)
	select {
	case <-ticker.C:

	case <-ctx.Done():
		err = ctx.Err()
	}

	ticker.Stop()
	return
}
