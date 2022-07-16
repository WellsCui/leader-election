package locks

import (
	"context"
	"time"
)

type (
	Lock struct {
		ID           string     `bson:"id"`
		Resource     string     `bson:"resource"`
		Owner        string     `bson:"owner"`
		AcquiredTime *time.Time `bson:"acquiredTime"`
		UpdatedAt    *time.Time `bson:"updatedAt"`
		Expiry       *time.Time `bson:"expiry"`
		Priority     *int       `bson:"priority"`
	}

	LockOption interface {
		Apply(options *lockOptions)
	}

	RetryInterval func(retries int, lastError error) (bool, time.Duration)

	WithRetry RetryInterval

	lockOptions struct {
		Owner        string
		Expiry       *time.Duration
		UpdateExpiry *time.Duration
		RetryOption  WithRetry
		Priority     *WithPriority
	}

	Locker interface {
		Lock(ctx context.Context, resource string, options ...LockOption) (*Lock, error)
		Unlock(ctx context.Context, lock *Lock) error
		Renew(ctx context.Context, lock *Lock) error
		Find(ctx context.Context, resource string) (*Lock, error)
	}

	WithOwner        string
	WithExpiry       time.Duration
	WithUpdateExpiry time.Duration
	WithPriority     int
)

var (
	defaultExpiry = time.Hour
)

func buildOptions(opts []LockOption) *lockOptions {
	rs := &lockOptions{
		Expiry: &defaultExpiry,
	}
	for _, opt := range opts {
		opt.Apply(rs)
	}
	return rs
}

func (option WithOwner) Apply(options *lockOptions) {
	options.Owner = string(option)
}

func (expiry WithExpiry) Apply(options *lockOptions) {
	options.Expiry = (*time.Duration)(&expiry)
}

func (expiry WithUpdateExpiry) Apply(options *lockOptions) {
	options.UpdateExpiry = (*time.Duration)(&expiry)
}

func (option WithRetry) Apply(options *lockOptions) {
	options.RetryOption = option
}

func (option WithPriority) Apply(options *lockOptions) {
	options.Priority = &option
}

func (l *Lock) Expired(updateExpiry *time.Duration) bool {
	expired := l.Expiry != nil && time.Now().After(*l.Expiry)
	updateDeadLine := time.Now().Add(-*updateExpiry)
	updateExpired := updateExpiry != nil && (l.UpdatedAt == nil || l.UpdatedAt.Before(updateDeadLine))
	return expired || updateExpired
}

// Retry executes action with retries
func Retry(ctx context.Context, action func() error, conditionalInterval RetryInterval) error {
	var err error
	retries := 0
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err = action()
		if err == nil {
			return nil
		}
		shouldRetry, retryInterval := conditionalInterval(retries, err)
		if !shouldRetry {
			return err
		}
		retries++
		time.Sleep(retryInterval)
	}
}
