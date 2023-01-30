package raft

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	_ "github.com/mattn/go-sqlite3"
)

// Storage backend backed by SQLite
type SqliteStorage struct {
	db *sql.DB
}

// Put default values into database
func (s *SqliteStorage) initDB() {
	tx, err := s.db.Begin()
	if err != nil {
		panic(err)
	}

	defer tx.Commit()

	if _, err := tx.Exec("CREATE TABLE IF NOT EXISTS \"metadata\" (name TEXT UNIQUE, value)"); err != nil {
		tx.Rollback()
		panic(err)
	}

	if _, err := tx.Exec("INSERT INTO \"metadata\" (name, value) VALUES ('term', 0) ON CONFLICT DO NOTHING"); err != nil {
		tx.Rollback()
		panic(err)
	}

	if _, err := tx.Exec("INSERT INTO \"metadata\" (name, value) VALUES ('votedFor', NULL) ON CONFLICT DO NOTHING"); err != nil {
		tx.Rollback()
		panic(err)
	}

	if _, err := tx.Exec("CREATE TABLE IF NOT EXISTS \"log\" (\"index\" INTEGER UNIQUE, term INTEGER, client_id INTEGER, request_id INTEGER, entry BLOB)"); err != nil {
		tx.Rollback()
		panic(err)
	}
}

// Make new SQLite storage from filename
func NewSqliteStorage(filename string) *SqliteStorage {
	db, err := sql.Open("sqlite3", filename)
	if err != nil {
		panic(err)
	}

	s := &SqliteStorage{
		db: db,
	}

	s.initDB()

	return s
}

// Close storage
func (s *SqliteStorage) Close() {
	s.db.Close()
}

// Executes a given transaction against the backend
func (s *SqliteStorage) Execute(t *Transaction) (interface{}, error) {
	// Do it all in a transaction so it can be applied atomically and be rolled back
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Commit()

	// Run the transaction
	value, err := t.f(tx)

	// If there was an error, roll it back
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	// Otherwise return the result
	return value, nil
}

// Get the current term
func (s *SqliteStorage) GetTerm() *Transaction {
	return &Transaction{
		f: func(ctx interface{}) (interface{}, error) {
			tx := ctx.(*sql.Tx)
			row := tx.QueryRow("SELECT value FROM \"metadata\" WHERE name='term'")

			var term int
			if err := row.Scan(&term); err != nil {
				return nil, err
			}

			return term, nil
		},
	}
}

// Get the chosen candidate
func (s *SqliteStorage) GetVotedFor() *Transaction {
	return &Transaction{
		f: func(ctx interface{}) (interface{}, error) {
			tx := ctx.(*sql.Tx)
			row := tx.QueryRow("SELECT value FROM \"metadata\" WHERE name='votedFor'")

			var votedFor sql.NullString
			if err := row.Scan(&votedFor); err != nil {
				return nil, err
			}

			return []interface{}{votedFor.Valid, votedFor.String}, nil
		},
	}
}

// Get the log
func (s *SqliteStorage) GetLog() *Transaction {
	return &Transaction{
		f: func(ctx interface{}) (interface{}, error) {
			tx := ctx.(*sql.Tx)
			rows, err := tx.Query("SELECT term, client_id, request_id, entry FROM \"log\" ORDER BY \"index\" ASC")
			if err != nil {
				return nil, err
			}

			defer rows.Close()

			var log []LogEntry

			for rows.Next() {
				var term int
				var clientId int64
				var requestId int64
				var blob []uint8
				if err := rows.Scan(&term, &clientId, &requestId, &blob); err != nil {
					return nil, err
				}

				r := bytes.NewBuffer(blob)
				d := gob.NewDecoder(r)

				var data interface{}
				if err := d.Decode(&data); err != nil {
					return nil, err
				}

				log = append(log, LogEntry{
					ClientId:  clientId,
					RequestId: requestId,
					Term:      term,
					Data:      data,
				})
			}

			return log, nil
		},
	}
}

// Set the current term
func (s *SqliteStorage) SetTerm(term int) *Transaction {
	return &Transaction{
		f: func(ctx interface{}) (interface{}, error) {
			tx := ctx.(*sql.Tx)
			if _, err := tx.Exec("UPDATE \"metadata\" SET VALUE=? WHERE name='term'", term); err != nil {
				return nil, err
			}
			return nil, nil
		},
	}
}

// Set the chosen candidate
func (s *SqliteStorage) SetVotedFor(haveVoted bool, votedFor string) *Transaction {
	return &Transaction{
		f: func(ctx interface{}) (interface{}, error) {
			tx := ctx.(*sql.Tx)
			ns := sql.NullString{
				String: votedFor,
				Valid:  haveVoted,
			}

			if _, err := tx.Exec("UPDATE \"metadata\" SET VALUE=? WHERE name='votedFor'", ns); err != nil {
				return nil, err
			}

			return nil, nil
		},
	}
}

// Append an entry to the log
func (s *SqliteStorage) AppendLog(entry LogEntry) *Transaction {
	return &Transaction{
		f: func(ctx interface{}) (interface{}, error) {
			tx := ctx.(*sql.Tx)
			var blob bytes.Buffer
			e := gob.NewEncoder(&blob)
			if err := e.Encode(&entry.Data); err != nil {
				return nil, err
			}

			_, err := tx.Exec("INSERT INTO \"log\" (\"index\", term, client_id, request_id, entry) SELECT COALESCE(MAX(log.\"index\"), -1) + 1, ?, ?, ?, ? FROM \"log\"", entry.Term, entry.ClientId, entry.RequestId, blob.Bytes())
			if err != nil {
				return nil, err
			}

			return nil, nil
		},
	}
}

// Trim the log from a given index
func (s *SqliteStorage) TrimLog(trimFrom int) *Transaction {
	return &Transaction{
		f: func(ctx interface{}) (interface{}, error) {
			tx := ctx.(*sql.Tx)
			_, err := tx.Exec("DELETE FROM \"log\" WHERE \"index\" >= ?", trimFrom)
			if err != nil {
				return nil, err
			}

			return nil, nil
		},
	}
}
