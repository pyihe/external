package internal

import (
	"log"
	"runtime"
)

const (
	stackSize = 4096
)

// ExecWithRecover 执行函数f，并捕获异常
func ExecWithRecover(f func()) {
	defer func() {
		if err := recover(); err != nil {
			buf := make([]byte, stackSize)
			n := runtime.Stack(buf, false)
			log.Printf("%s\n", buf[:n])
		}
	}()

	f()
}
