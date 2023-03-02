package etcdlocker

import (
	"context"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/pyihe/external/distributed/locker"
)

const (
	tolerance         = 1
	defaultLockExpire = 60
)

type Mutex struct {
	m *concurrency.Mutex
}

// NewMutex 新建一把锁
func NewMutex(client *clientv3.Client, key string, ttlSec int) (*Mutex, error) {
	if ttlSec <= 0 {
		ttlSec = defaultLockExpire
	}
	session, err := concurrency.NewSession(client, concurrency.WithTTL(ttlSec+tolerance))
	if err != nil {
		return nil, err
	}

	return &Mutex{concurrency.NewMutex(session, key)}, err
}

func (mutex *Mutex) Key() string {
	return mutex.m.Key()
}

func (mutex *Mutex) Lock(ctx context.Context, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = locker.DefaultLockTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return mutex.m.Lock(ctx)
}

func (mutex *Mutex) TryLock(ctx context.Context) error {
	return mutex.m.TryLock(ctx)
}

func (mutex *Mutex) Unlock(ctx context.Context) error {
	return mutex.m.Unlock(ctx)
}
