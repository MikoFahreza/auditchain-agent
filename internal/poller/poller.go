package poller

import (
	"context"
	"fmt"
	"log"
	"time"

	"auditchain-agent/internal/config"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// LogEntry adalah hasil polling dari satu baris yang berubah
type LogEntry struct {
	Actor        string
	Action       string
	Resource     string
	Timestamp    time.Time
	SourceSystem string
	Metadata     map[string]interface{}
}

type Poller struct {
	db  *pgxpool.Pool
	cfg *config.Config
}

func New(db *pgxpool.Pool, cfg *config.Config) *Poller {
	return &Poller{db: db, cfg: cfg}
}

// PollTable mengambil baris yang berubah sejak last_polled dari tabel tertentu
func (p *Poller) PollTable(ctx context.Context, table config.TableConfig) ([]LogEntry, error) {
	// 1. Ambil checkpoint terakhir untuk tabel ini
	var lastPolled time.Time
	err := p.db.QueryRow(ctx,
		"SELECT last_polled FROM agent_checkpoints WHERE table_name = $1",
		table.Name,
	).Scan(&lastPolled)
	if err != nil {
		return nil, fmt.Errorf("gagal baca checkpoint tabel %s: %w", table.Name, err)
	}

	// 2. Query baris yang modified_at > lastPolled
	query := fmt.Sprintf(
		"SELECT * FROM %s WHERE modified_at > $1 ORDER BY modified_at ASC LIMIT $2",
		table.Name,
	)

	rows, err := p.db.Query(ctx, query, lastPolled, p.cfg.Polling.BatchSize)
	if err != nil {
		return nil, fmt.Errorf("gagal polling tabel %s: %w", table.Name, err)
	}
	defer rows.Close()

	entries, latestTime, err := p.parseRows(rows, table, lastPolled)
	if err != nil {
		return nil, err
	}

	if len(entries) == 0 {
		return nil, nil
	}

	// 3. Update checkpoint ke modified_at terbaru yang sudah diambil
	_, err = p.db.Exec(ctx,
		"UPDATE agent_checkpoints SET last_polled = $1 WHERE table_name = $2",
		latestTime, table.Name,
	)
	if err != nil {
		return nil, fmt.Errorf("gagal update checkpoint tabel %s: %w", table.Name, err)
	}

	log.Printf("[Poller] ✅ Tabel %s: %d baris baru ditemukan", table.Name, len(entries))
	return entries, nil
}

// parseRows mengubah hasil query menjadi slice LogEntry
func (p *Poller) parseRows(rows pgx.Rows, table config.TableConfig, lastPolled time.Time) ([]LogEntry, time.Time, error) {
	fieldDescs := rows.FieldDescriptions()
	latestTime := lastPolled
	var entries []LogEntry

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, latestTime, fmt.Errorf("gagal baca baris: %w", err)
		}

		// Mapping kolom ke map
		rowMap := make(map[string]interface{})
		for i, fd := range fieldDescs {
			rowMap[string(fd.Name)] = values[i]
		}

		// Ambil nilai actor, resource, timestamp dari mapping config
		actor := fmt.Sprintf("%v", rowMap[table.ActorField])
		resource := fmt.Sprintf("%s:%s", table.Name, fmt.Sprintf("%v", rowMap[table.ResourceField]))

		// Tentukan action berdasarkan konteks
		// Karena polling, semua yang muncul dianggap UPDATE atau INSERT
		// Agent tidak bisa bedakan INSERT vs UPDATE dari polling biasa
		// Gunakan konvensi: kalau modified_at == created_at → INSERT, selainnya → UPDATE
		action := "UPDATE"
		if createdAt, ok := rowMap["created_at"]; ok {
			if modifiedAt, ok2 := rowMap["modified_at"]; ok2 {
				if fmt.Sprintf("%v", createdAt) == fmt.Sprintf("%v", modifiedAt) {
					action = "INSERT"
				}
			}
		}

		var ts time.Time
		if modifiedAt, ok := rowMap["modified_at"]; ok {
			if t, ok := modifiedAt.(time.Time); ok {
				ts = t
				if t.After(latestTime) {
					latestTime = t
				}
			}
		}

		// Hapus field internal dari metadata agar tidak redundan
		delete(rowMap, table.ActorField)
		delete(rowMap, "modified_at")
		delete(rowMap, "modified_by")

		entries = append(entries, LogEntry{
			Actor:        actor,
			Action:       action,
			Resource:     resource,
			Timestamp:    ts,
			SourceSystem: table.SourceSystem,
			Metadata:     rowMap,
		})
	}

	return entries, latestTime, rows.Err()
}
