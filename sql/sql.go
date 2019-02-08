package sql

import (
	"context"
	"database/sql"
	"time"

	"github.com/pkg/errors"

	"github.com/mattermost/mattermost-server/mlog"
	"github.com/mattermost/mattermost-server/model"
)

const (
	DB_PING_ATTEMPTS     = 18
	DB_PING_TIMEOUT_SECS = 10
)

// Open creates a sql.DB instance to the given data source after verifying connectivity via pings.
func Open(driverName, dataSourceName string) (*sql.DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	for i := 0; i < DB_PING_ATTEMPTS; i++ {
		if i > 0 {
			mlog.Info("Pinging database")
		}

		ctx, cancel := context.WithTimeout(context.Background(), DB_PING_TIMEOUT_SECS*time.Second)
		defer cancel()

		err = db.PingContext(ctx)
		if err == nil {
			break
		}

		if i == DB_PING_ATTEMPTS-1 {
			return nil, errors.Wrap(err, "failed to ping db")
		} else {
			mlog.Error("Failed to ping DB; retrying", mlog.Int("wait_seconds", DB_PING_TIMEOUT_SECS), mlog.Err(err))
			time.Sleep(DB_PING_TIMEOUT_SECS * time.Second)
		}
	}

	return db, nil
}

// ApplySettings applies sql settings to a previously opened *sql.DB instance.
func ApplySettings(db *sql.DB, settings *model.SqlSettings) {
	db.SetMaxIdleConns(*settings.MaxIdleConns)
	db.SetMaxOpenConns(*settings.MaxOpenConns)
	db.SetConnMaxLifetime(time.Duration(*settings.ConnMaxLifetimeMilliseconds) * time.Millisecond)
}
