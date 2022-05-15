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
				t.Logf("lock: %+v", lock)
				t.Logf("lock error: %+v", err)
				require.NoError(t, err)
				require.NotNil(t, lock)
			},
		},
		{
			name: "should fail to lock if lock is not expired",
			expects: func(t *testing.T) {
				lock, err := locker.Lock(ctx, "leadership2", locks.WithOwner("owner1"),
					locks.WithExpiry(time.Hour),
					locks.WithRenewExpiry(time.Minute))
				t.Logf("lock: %+v", lock)
				t.Logf("lock error: %+v", err)
				require.NoError(t, err)
				require.NotNil(t, lock)
				lock, err = locker.Lock(ctx, "leadership2", locks.WithOwner("owner2"),
					locks.WithExpiry(time.Hour),
					locks.WithRenewExpiry(locks.WithRenewExpiry(time.Minute)))
				t.Logf("lock: %+v", lock)
				t.Logf("lock error: %+v", err)
				require.Error(t, err)
				require.Nil(t, lock)
			},
		},
		{
			name: "should lock successfully if lock is expired",
			expects: func(t *testing.T) {
				lock, err := locker.Lock(ctx, "leadership3", locks.WithOwner("owner1"),
					locks.WithExpiry(time.Hour),
					locks.WithRenewExpiry(time.Minute))
				t.Logf("lock: %+v", lock)
				t.Logf("lock error: %+v", err)
				require.NoError(t, err)
				require.NotNil(t, lock)
				time.Sleep(time.Millisecond)
				lock, err = locker.Lock(ctx, "leadership3", locks.WithOwner("owner2"),
					locks.WithExpiry(time.Hour),
					locks.WithRenewExpiry(time.Millisecond))
				t.Logf("lock: %+v", lock)
				t.Logf("lock error: %+v", err)
				require.NoError(t, err)
				require.NotNil(t, lock)
			},
		},
		{
			name: "should retry until success",
			expects: func(t *testing.T) {
				lock, err := locker.Lock(ctx, "leadership4", locks.WithOwner("owner1"),
					locks.WithExpiry(time.Hour),
					locks.WithRenewExpiry(time.Minute))
				t.Logf("lock: %+v", lock)
				t.Logf("lock error: %+v", err)
				require.NoError(t, err)
				require.NotNil(t, lock)
				retryCount := 0
				lock, err = locker.Lock(ctx, "leadership4", locks.WithOwner("owner2"),
					locks.WithExpiry(time.Hour),
					locks.WithRenewExpiry(time.Second),
					&locks.RetryOption{
						RetryInterval: func(retries int, lastError error) (bool, time.Duration){
							if retries>10 {
								return false, time.Millisecond
							}
							retryCount++
							return true, time.Millisecond*300
						},
					})
				t.Logf("lock: %+v", lock)
				t.Logf("lock error: %+v", err)
				t.Logf("retry count: %+v", retryCount)
				require.NoError(t, err)
				require.NotNil(t, lock)
				assert.True(t, retryCount>3)
			},
		},
	}
	for _, cs := range cases {
		t.Run(cs.name, cs.expects)
	}
}
