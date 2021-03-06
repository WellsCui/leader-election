package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wellscui/leader-election/locks"
)

func TestMongoDBLocker(t *testing.T) {
	ctx := context.Background()
	db, err := getMongoDB(ctx, "test")
	require.NoError(t, err)
	locker := locks.NewMongoDBLocker(db)
	cases := []struct {
		name    string
		expects func(t *testing.T)
	}{
		{
			name: "should be able to create lock",
			expects: func(t *testing.T) {
				lock, err := locker.Lock(ctx, "leadership1")
				t.Log("lock:", lock)
				require.NoError(t, err)
				require.NotNil(t, lock)
			},
		},
		{
			name: "should fail to lock if lock is not expired",
			expects: func(t *testing.T) {
				originalLock, err := locker.Lock(ctx, "leadership2", locks.WithOwner("owner1"),
					locks.WithExpiry(time.Hour),
					locks.WithUpdateExpiry(time.Minute))
				require.NoError(t, err)
				require.NotNil(t, originalLock)
				t.Log("lock:", originalLock)
				updatedLock, err := locker.Lock(ctx, "leadership2", locks.WithOwner("owner2"),
					locks.WithExpiry(time.Hour),
					locks.WithUpdateExpiry(locks.WithUpdateExpiry(time.Minute)))
				require.Error(t, err)
				require.Equal(t, originalLock, updatedLock)
			},
		},
		{
			name: "should succeed when lock is not updated in time",
			expects: func(t *testing.T) {
				originalLock, err := locker.Lock(ctx, "leadership2", locks.WithOwner("owner1"),
					locks.WithExpiry(time.Hour),
					locks.WithUpdateExpiry(time.Millisecond))
				require.NoError(t, err)
				require.NotNil(t, originalLock)
				t.Log("lock:", originalLock)
				time.Sleep(time.Millisecond)
				updatedLock, err := locker.Lock(ctx, "leadership2", locks.WithOwner("owner2"),
					locks.WithExpiry(time.Hour),
					locks.WithUpdateExpiry(locks.WithUpdateExpiry(time.Millisecond)))
				require.NoError(t, err)
				require.NotEqual(t, originalLock, updatedLock)
				require.Equal(t, "owner2", updatedLock.Owner)
			},
		},
		{
			name: "should succeed with higher priority",
			expects: func(t *testing.T) {
				originalLock, err := locker.Lock(ctx, "leadership2", locks.WithOwner("owner1"),
					locks.WithExpiry(time.Hour),
					locks.WithUpdateExpiry(time.Minute),
					locks.WithPriority(1),
				)
				require.NoError(t, err)
				require.NotNil(t, originalLock)
				t.Log("lock:", originalLock)
				updatedLock, err := locker.Lock(ctx, "leadership2", locks.WithOwner("owner2"),
					locks.WithExpiry(time.Hour),
					locks.WithUpdateExpiry(locks.WithUpdateExpiry(time.Minute)),
					locks.WithPriority(2),
				)
				require.NoError(t, err)
				require.NotEqual(t, originalLock, updatedLock)
				require.Equal(t, "owner2", updatedLock.Owner)
				require.Equal(t, 2, *updatedLock.Priority)
			},
		},
		{
			name: "should be able to lock after unlock",
			expects: func(t *testing.T) {
				lock, err := locker.Lock(ctx, "leadership3", locks.WithOwner("owner1"),
					locks.WithExpiry(time.Hour),
					locks.WithUpdateExpiry(time.Minute))
				require.NoError(t, err)
				require.NotNil(t, lock)
				err = locker.Unlock(ctx, lock)
				require.NoError(t, err)
				lock, err = locker.Lock(ctx, "leadership3", locks.WithOwner("owner2"),
					locks.WithExpiry(time.Hour),
					locks.WithUpdateExpiry(locks.WithUpdateExpiry(time.Minute)))
				require.NoError(t, err)
				require.NotNil(t, lock)
			},
		},
		{
			name: "should lock successfully if lock is expired",
			expects: func(t *testing.T) {
				originalLock, err := locker.Lock(ctx, "leadership4", locks.WithOwner("owner1"),
					locks.WithExpiry(time.Millisecond*200),
					locks.WithUpdateExpiry(time.Millisecond*200))
				require.NoError(t, err)
				require.NotNil(t, originalLock)
				time.Sleep(time.Second)
				newlock, err := locker.Lock(ctx, "leadership4", locks.WithOwner("owner2"),
					locks.WithExpiry(time.Millisecond*200),
					locks.WithUpdateExpiry(time.Millisecond*200))
				require.NoError(t, err)
				require.NotEqual(t, originalLock, newlock)
				require.Equal(t, "owner2", newlock.Owner)
			},
		},
		{
			name: "should retry until success",
			expects: func(t *testing.T) {
				lock, err := locker.Lock(ctx, "leadership5", locks.WithOwner("owner1"),
					locks.WithExpiry(time.Second),
					locks.WithUpdateExpiry(time.Second))
				t.Logf("lock: %+v", lock)
				t.Logf("lock error: %+v", err)
				require.NoError(t, err)
				require.NotNil(t, lock)
				retryCount := 0
				lock, err = locker.Lock(ctx, "leadership5", locks.WithOwner("owner2"),
					locks.WithExpiry(time.Second),
					locks.WithUpdateExpiry(time.Second),
					locks.WithRetry(func(retries int, lastError error) (bool, time.Duration) {
						if retries > 10 {
							return false, time.Millisecond
						}
						retryCount++
						return true, time.Millisecond * 300
					}))
				t.Logf("lock: %+v", lock)
				t.Logf("lock error: %+v", err)
				t.Logf("retry count: %+v", retryCount)
				require.NoError(t, err)
				require.NotNil(t, lock)
				assert.True(t, retryCount > 3)
			},
		},
		{
			name: "should fail to lock after the renew",
			expects: func(t *testing.T) {
				lock, err := locker.Lock(ctx, "leadership6", locks.WithOwner("owner1"),
					locks.WithExpiry(time.Hour),
					locks.WithUpdateExpiry(time.Second))
				t.Logf("lock: %+v", lock)
				t.Logf("lock error: %+v", err)
				require.NoError(t, err)
				require.NotNil(t, lock)
				time.Sleep(time.Millisecond * 800)
				require.NoError(t, locker.Renew(ctx, lock))
				lock, err = locker.Lock(ctx, "leadership6", locks.WithOwner("owner2"),
					locks.WithExpiry(time.Hour),
					locks.WithUpdateExpiry(time.Second))
				t.Logf("lock: %+v", lock)
				t.Logf("lock error: %+v", err)
				require.Error(t, err)
				require.Equal(t, "owner1", lock.Owner)
			},
		},
	}
	for _, cs := range cases {
		t.Run(cs.name, cs.expects)
	}
}
