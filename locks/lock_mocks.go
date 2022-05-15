package locks

import (
	"context"
	"github.com/stretchr/testify/mock"
)

type MockedLocker struct {
	mock.Mock
}

// CreateFailedReloadRecord mock CreateFailedReloadRecord in ReloadService
func (m *MockedLocker) Lock(ctx context.Context, resource string, options ...LockOption) (*Lock, error) {
	params:= []interface{}{ctx, resource}
	for _, opt:= range options {
		params = append(params, opt)
	}
	
	returns := m.Mock.Called(params...)
	lock, _ := returns[0].(*Lock)
	err, _:= returns[1].(error)
	return lock, err
}

func (m *MockedLocker) Unlock(ctx context.Context, lock *Lock) error {
	returns := m.Mock.Called(ctx, lock)
	err, _ := returns[0].(error)
	return err
}

func (m *MockedLocker) Renew(ctx context.Context, lock *Lock) error {
	returns := m.Mock.Called(ctx, lock)
	err, _ := returns[0].(error)
	return err
}
