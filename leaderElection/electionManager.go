package leaderelection

import (
	"context"
	"sync"
	"time"

	"github.com/wellscui/leader-election/locks"
)

type (
	// Election defines the attributes of an election
	Election struct {
		Name    string
		Tenure  time.Duration
		Renewal time.Duration
	}

	// Manager manages the elections and tracks the leaders of the election
	Manager struct {
		mutex     sync.RWMutex
		elections []Election
		leaders   map[string]string
		locker    locks.Locker
		candidate string
	}
)

func NewManger(candidate string, elections []Election, locker locks.Locker) *Manager {
	rs := &Manager{
		elections: elections,
		locker:    locker,
		leaders:   map[string]string{},
		candidate: candidate,
	}
	return rs
}

func (m *Manager) Start(ctx context.Context) error {
	wg := sync.WaitGroup{}
	wg.Add(len(m.elections))
	for _, election := range m.elections {
		go func(election Election) {
			m.launchElection(ctx, election)
			wg.Done()
		}(election)
	}
	wg.Wait()
	return nil
}

func (m *Manager) updateLeader(election string, leader string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.leaders[election] = leader
}

func (m *Manager) IsLeader(election string) bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.leaders[election] == m.candidate
}

func (m *Manager) launchElection(ctx context.Context, election Election) error {
	ticker := time.NewTicker(election.Renewal / 2)
	defer ticker.Stop()
	var err error
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			_, err = m.locker.Lock(ctx, election.Name,
				locks.WithOwner(m.candidate),
				locks.WithExpiry(election.Tenure),
				locks.WithUpdateExpiry(election.Renewal))

			leader := m.candidate
			if err != nil {
				leader = ""
			}
			m.updateLeader(election.Name, leader)
		}
	}
}
