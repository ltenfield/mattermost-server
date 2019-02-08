package sql

import (
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"

	"github.com/mattermost/mattermost-server/mlog"
	"github.com/mattermost/mattermost-server/model"
)

// Manager manages a collection of different kinds of database connections.
type Manager struct {
	// rrCounter and srCounter should be kept first.
	// See https://github.com/mattermost/mattermost-server/pull/7281
	rrCounter int64
	srCounter int64

	master         *sql.DB
	replicas       []*sql.DB
	searchReplicas []*sql.DB
	lockedToMaster bool
	lock           sync.RWMutex
}

// NewManager creates a new instance of manager.
func NewManager() *Manager {
	return &Manager{}
}

// Connect connects the master and any configured replicas.
//
// It may be safely called multiple times if the configuration changes to connect to new replicas,
// or tweak connectivity settings, but will reject any change that requires closing the master
// connection.
func (m *Manager) Connect(sqlSettings *model.SqlSettings) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.master == nil {
		master, err := Open(*sqlSettings.DriverName, *sqlSettings.DataSource)
		if err != nil {
			return errors.Wrap(err, "failed to open master connection")
		}

		m.master = master
	}

	// type SqlSettings struct {
	// 	DriverName                  *string
	// 	DataSource                  *string
	// 	DataSourceReplicas          []string
	// 	DataSourceSearchReplicas    []string
	// 	MaxIdleConns                *int
	// 	ConnMaxLifetimeMilliseconds *int
	// 	MaxOpenConns                *int
	// 	Trace                       *bool
	// 	AtRestEncryptKey            *string
	// 	QueryTimeout                *int
	// }
}

// GetMaster returns the master database connection.
func (m *Manager) GetMaster() *sql.DB {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return m.master
}

func (m *Manager) getReplica() *sql.DB {
	if len(m.replicas) == 0 || m.lockedToMaster {
		return m.master
	}

	// Simple load balancing across the replicas.
	rrNum := atomic.AddInt64(&m.rrCounter, 1) % int64(len(m.replicas))
	return m.replicas[rrNum]
}

// GetReplica returns a database connection to use for read-only queries.
func (m *Manager) GetReplica() *sql.DB {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return m.getReplica()
}

// GetSearchReplica returns a database connection to use for read-only search queries.
func (m *Manager) GetSearchReplica() *sql.DB {
	m.lock.RLock()
	defer m.lock.RUnlock()

	if len(m.searchReplicas) == 0 {
		return m.getReplica()
	}

	// Simple load balancing across the search replicas.
	rrNum := atomic.AddInt64(&m.srCounter, 1) % int64(len(m.searchReplicas))
	return m.searchReplicas[rrNum]
}

func (m *Manager) LockToMaster() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.lockedToMaster = true
}

func (m *Manager) UnlockFromMaster() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.lockedToMaster = false
}

// Close closes all open database connections.
func (m *Manager) Close() {
	m.lock.Lock()
	defer m.lock.Unlock()

	if err := m.master.Close(); err != nil {
		mlog.Error("Failed to close master database connection", mlog.Err(err))
	}
	for i, replica := range m.replicas {
		if err := replica.Close(); err != nil {
			mlog.Error(fmt.Sprintf("Failed to close replica database connection #%d", i), mlog.Err(err))
		}
	}

	m.master = nil
	m.replicas = []*sql.DB{}
}
