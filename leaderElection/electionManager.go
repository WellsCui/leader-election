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
		getPriority func() int
	}
)

func NewManger(candidate string, elections []Election, locker locks.Locker, getPriority func() int) *Manager {
	rs := &Manager{
		elections: elections,
		locker:    locker,
		leaders:   map[string]string{},
		candidate: candidate,
		getPriority: getPriority,
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

func (m *Manager) IsLeader(ctx context.Context, election string) bool {
	leader, _ := m.FindLeader(ctx, election)
	return leader == m.candidate
}

func (m *Manager) FindLeader(ctx context.Context, election string) (string, error) {
	lock, err := m.locker.Find(ctx, election)
	if err != nil {
		return "", err
	}

	return lock.Owner, nil
}

func (m *Manager) launchElection(ctx context.Context, election Election) error {
	ticker := time.NewTicker(election.Renewal / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			lock, _ := m.locker.Lock(ctx, election.Name,
				locks.WithOwner(m.candidate),
				locks.WithExpiry(election.Tenure),
				locks.WithUpdateExpiry(election.Renewal),
				locks.WithPriority(m.getPriority()),
			)
			if lock!=nil {
				m.updateLeader(election.Name, lock.Owner)
			}
		}
	}
}
