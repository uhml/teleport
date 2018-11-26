package lite

import (
	"database/sql"
	"time"

	"github.com/gravitational/teleport/lib/backend"

	"github.com/gravitational/trace"
)

const notSet = -2

func (l *LiteBackend) runPeriodicOperations() {
	t := time.NewTicker(l.PollStreamPeriod)
	defer t.Stop()

	rowid := int64(notSet)
	for {
		select {
		case <-l.ctx.Done():
			return
		case <-t.C:
			err := l.removeExpiredKeys()
			if err != nil {
				l.Warningf("Failed to run remove expired keys: %v", err)
			}
			if !l.EventsOff {
				err = l.removeOldEvents()
				if err != nil {
					l.Warningf("Failed to run remove old events: %v", err)
				}
				rowid, err = l.pollEvents(rowid)
				if err != nil {
					l.Warningf("Failed to run poll events: %v", err)
				}
			}
		}
	}
}

func (l *LiteBackend) removeExpiredKeys() error {
	now := l.clock.Now().UTC()
	return l.inTransaction(l.ctx, func(tx *sql.Tx) error {
		q, err := tx.Prepare(
			"SELECT key FROM kv WHERE expires <= ? ORDER BY key LIMIT ?")
		if err != nil {
			return trace.Wrap(err)
		}
		rows, err := q.Query(now, l.BufferSize)
		if err != nil {
			return trace.Wrap(err)
		}
		defer rows.Close()
		var keys [][]byte
		for rows.Next() {
			var key []byte
			if err := rows.Scan(&key); err != nil {
				return trace.Wrap(err)
			}
			keys = append(keys, key)
		}

		for i := range keys {
			if err := l.deleteInTransaction(l.ctx, keys[i], tx); err != nil {
				return trace.Wrap(err)
			}
		}

		return nil
	})
}

func (l *LiteBackend) removeOldEvents() error {
	expiryTime := l.clock.Now().UTC().Add(-1 * backend.DefaultEventsTTL)
	return l.inTransaction(l.ctx, func(tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(l.ctx, "DELETE FROM events WHERE created <= ?")
		if err != nil {
			return trace.Wrap(err)
		}
		_, err = stmt.ExecContext(l.ctx, expiryTime)
		if err != nil {
			return trace.Wrap(err)
		}
		return nil
	})
}

func (l *LiteBackend) pollEvents(rowid int64) (int64, error) {
	if rowid == notSet {
		err := l.inTransaction(l.ctx, func(tx *sql.Tx) error {
			q, err := tx.PrepareContext(
				l.ctx,
				"SELECT id from events ORDER BY id DESC LIMIT 1")
			if err != nil {
				return trace.Wrap(err)
			}
			row := q.QueryRow()
			if err := row.Scan(&rowid); err != nil {
				if err != sql.ErrNoRows {
					return trace.Wrap(err)
				}
				rowid = -1
			} else {
				rowid = rowid - 1
			}
			return nil
		})
		if err != nil {
			return rowid, trace.Wrap(err)
		}
		l.Debugf("Initialized event ID iterator to %v", rowid)
		l.signalWatchStart()
	}

	var events []backend.Event
	var lastID int64
	err := l.inTransaction(l.ctx, func(tx *sql.Tx) error {
		q, err := tx.PrepareContext(l.ctx,
			"SELECT id, type, kv_key, kv_value, kv_modified FROM events WHERE id > ? ORDER BY id LIMIT ?")
		if err != nil {
			return trace.Wrap(err)
		}
		limit := l.BufferSize / 2
		if limit <= 0 {
			limit = 1
		}
		rows, err := q.Query(rowid, limit)
		if err != nil {
			return trace.Wrap(err)
		}
		defer rows.Close()
		for rows.Next() {
			var event backend.Event
			if err := rows.Scan(&lastID, &event.Type, &event.Item.Key, &event.Item.Value, &event.Item.ID); err != nil {
				return trace.Wrap(err)
			}
			events = append(events, event)
		}
		return nil
	})
	if err != nil {
		return rowid, trace.Wrap(err)
	}
	l.buf.PushBatch(events)
	if len(events) != 0 {
		return lastID, nil
	}
	return rowid, nil
}
