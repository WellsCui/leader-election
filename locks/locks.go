package locks

import (
	"context"
	"fmt"
	"time"
)

type (
	Lock struct {
		ID           string     `json:"id"`
		Resource     string     `json:"resource"`
		Owner        string     `json:"owner"`
		AcquiredTime *time.Time `json:"acquiredTime"`
		UpdatedAt    *time.Time `json:"updatedAt"`
		Expiry       *time.Time `json:"expiry"`
	}

	LockOption interface {
		Apply(options *LockOptions)
	}

	RetryInterval func(retries int, lastError error) (bool, time.Duration)

	WithRetry RetryInterval

	LockOptions struct {
		Owner       string
		Expiry      *time.Duration
		RenewExpiry *time.Duration
		RetryOption WithRetry
	}

	Locker interface {
		Lock(ctx context.Context, resource string, options ...LockOption) (*Lock, error)
		Unlock(ctx context.Context, lock *Lock) error
		Renew(ctx context.Context, lock *Lock) error
	}

	WithOwner       string
	WithExpiry      time.Duration
	WithRenewExpiry time.Duration
)

func buildOptions(opts []LockOption) *LockOptions {
	rs := &LockOptions{}
	for _, opt := range opts {
		opt.Apply(rs)
	}
	return rs
}

func (option WithOwner) Apply(options *LockOptions) {
	options.Owner = string(option)
}

func (expiry WithExpiry) Apply(options *LockOptions) {
	options.Expiry = (*time.Duration)(&expiry)
}

func (expiry WithRenewExpiry) Apply(options *LockOptions) {
	options.RenewExpiry = (*time.Duration)(&expiry)
}

func (option WithRetry) Apply(options *LockOptions) {
	options.RetryOption = option
}

func (l *Lock) Expired(renewExpiry *time.Duration) bool {
	expired := l.Expiry != nil && time.Now().After(*l.Expiry)
	updateDeadLine := time.Now().Add(-*renewExpiry)
	updateExpired := renewExpiry != nil && (l.UpdatedAt == nil || l.UpdatedAt.Before(updateDeadLine))
	fmt.Printf("expired: %v, updateExpired: %v, renewDeadLine: %v, UpdatedAt: %v, updateDeadLine: %v \n",
		expired, updateExpired, renewExpiry, l.UpdatedAt, updateDeadLine)
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
