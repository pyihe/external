package concurrency

var (
	defaultPool = NewPool(0, 0)
)

func Stop(key string) {
	defaultPool.Stop(key)
}

func SetThreshold(n int32) {
	defaultPool.SetThreshold(n)
}

func SetRunnerCap(n int32) {
	defaultPool.SetRunnerCap(n)
}

func Go(key string, f func()) {
	defaultPool.Go(key, f)
}
