package leaderelection

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/wellscui/leader-election/locks"
)

func TestManager(t *testing.T) {
	cases := []struct {
		name      string
		elections []Election
		locker    *locks.MockedLocker
	}{
		{
			name: "should success",
			elections: []Election{
				{
					Name:    "leaderShip1",
					Tenure:  time.Second,
					Renewal: time.Millisecond * 100,
				},
			},
			locker: func() *locks.MockedLocker {
				rs := &locks.MockedLocker{}
				acquiredTime := time.Now()
				expiry := acquiredTime.Add(time.Second)
				lock := &locks.Lock{
					ID:           "lock1",
					Resource:     "leaderShip1",
					Owner:        "candidate1",
					AcquiredTime: &acquiredTime,
					UpdatedAt:    &acquiredTime,
					Expiry:       &expiry,
				}
				rs.Mock.On("Lock", mock.Anything, "leaderShip1",
					locks.WithOwner("candidate1"),
					locks.WithExpiry(time.Second),
					locks.WithUpdateExpiry(time.Millisecond*100)).Return(lock, nil)
				rs.Mock.On("Renew", mock.Anything, lock).Return(nil)
				return rs
			}(),
		},
	}

	for _, cs := range cases {
		t.Run(cs.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			manager := NewManger("candidate1", cs.elections, cs.locker)
			go func() {
				time.Sleep(time.Second * 2)
				cancel()
			}()
			manager.Start(ctx)
			cs.locker.AssertExpectations(t)
		})
	}
}
