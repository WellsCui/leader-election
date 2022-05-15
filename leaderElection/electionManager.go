package leaderelection

import (
	"context"
	"time"
	"github.com/wellscui/leader-election/locks"
	"sync"
)

type (
	Election struct {
		Name    string
		Tenure  time.Duration
		Renewal time.Duration
	}

	Manager struct {
		mutex sync.RWMutex
		elections []Election
		leaders map[string] string
		locker locks.Locker
		candidate string
	}
)

func NewManger(candidate string, elections []Election, locker locks.Locker) *Manager {
	rs:= &Manager{
		elections: elections,
		locker: locker,
		leaders: map[string]string{},
		candidate: candidate,
	}
	return rs
}

func(m *Manager) Start(ctx context.Context) error{
	wg:= sync.WaitGroup{}
	wg.Add(len(m.elections))
	for _, election := range m.elections {
		go func(election Election){
			m.LaunchElection(ctx, m.candidate, election)
			wg.Done()
		}(election)
	}
	wg.Wait()
	return nil
}

func (m *Manager) updateLeader(election string, leader string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.leaders[election]=leader
}

func (m *Manager) isLeader(election string) bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.leaders[election]==m.candidate
}

func (m *Manager) LaunchElection(ctx context.Context, candidate string, election Election) error{
	ticker:= time.NewTicker(election.Renewal)
	defer ticker.Stop()
	elected:=false
	var lock *locks.Lock
	var err error
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if elected {
				err = m.locker.Renew(ctx, lock)
			} else {
				lock, err = m.locker.Lock(ctx,election.Name, 
					locks.WithOwner(candidate),
					locks.WithExpiry(election.Tenure), 
					locks.WithRenewExpiry(election.Renewal))
			}
			
			elected = (err == nil)
			if lock!= nil {
				m.updateLeader(election.Name, lock.Owner)
			}
		}
	}
}

