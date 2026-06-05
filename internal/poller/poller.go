package poller

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"auditchain-agent/internal/config"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RawLogEntry adalah data mentah dari audit_trail.
// Penambahan: field ID (integer primary key dari audit_trail)
// agar dapat diteruskan ke Gateway sebagai audit_trail_id.
type RawLogEntry struct {
	ID           int                    // BARU: audit_trail.id
	Tabel        string                 `json:"tabel"`
	Operasi      string                 `json:"operasi"`
	DBUser       string                 `json:"db_user"`
	AppUser      *string                `json:"app_user"`
	DataLama     map[string]interface{} `json:"data_lama"`
	DataBaru     map[string]interface{} `json:"data_baru"`
	Waktu        time.Time              `json:"waktu"`
	SourceSystem string                 `json:"source_system"`
}

type Poller struct {
	db  *pgxpool.Pool
	cfg *config.Config
}

func New(db *pgxpool.Pool, cfg *config.Config) *Poller {
	return &Poller{db: db, cfg: cfg}
}

func (p *Poller) Poll(ctx context.Context) ([]RawLogEntry, error) {
	var lastIDStr string
	err := p.db.QueryRow(ctx,
		"SELECT value FROM agent_checkpoints WHERE key = 'audit_trail_last_id'",
	).Scan(&lastIDStr)
	if err != nil {
		return nil, fmt.Errorf("gagal baca checkpoint: %w", err)
	}

	var lastID int
	fmt.Sscanf(lastIDStr, "%d", &lastID)

	// BARU: sertakan id dalam SELECT
	rows, err := p.db.Query(ctx, `
		SELECT id, tabel, operasi, db_user, app_user, data_lama, data_baru, waktu
		FROM audit_trail
		WHERE id > $1
		ORDER BY id ASC
		LIMIT $2
	`, lastID, p.cfg.Polling.BatchSize)
	if err != nil {
		return nil, fmt.Errorf("gagal polling audit_trail: %w", err)
	}
	defer rows.Close()

	var entries []RawLogEntry
	var latestID int

	for rows.Next() {
		var (
			id                       int
			tabel, operasi           string
			dbUser                   string
			appUser                  *string
			dataLamaRaw, dataBaruRaw *string
			waktu                    time.Time
		)

		if err := rows.Scan(&id, &tabel, &operasi, &dbUser, &appUser, &dataLamaRaw, &dataBaruRaw, &waktu); err != nil {
			log.Printf("[Poller] ⚠️ Gagal scan baris: %v", err)
			continue
		}

		latestID = id

		var dataLama, dataBaru map[string]interface{}
		if dataLamaRaw != nil {
			json.Unmarshal([]byte(*dataLamaRaw), &dataLama)
		}
		if dataBaruRaw != nil {
			json.Unmarshal([]byte(*dataBaruRaw), &dataBaru)
		}

		entries = append(entries, RawLogEntry{
			ID:           id, // BARU
			Tabel:        tabel,
			Operasi:      operasi,
			DBUser:       dbUser,
			AppUser:      appUser,
			DataLama:     dataLama,
			DataBaru:     dataBaru,
			Waktu:        waktu,
			SourceSystem: p.getSourceSystem(tabel),
		})
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, nil
	}

	_, err = p.db.Exec(ctx,
		"UPDATE agent_checkpoints SET value = $1 WHERE key = 'audit_trail_last_id'",
		fmt.Sprintf("%d", latestID),
	)
	if err != nil {
		return nil, fmt.Errorf("gagal update checkpoint: %w", err)
	}

	log.Printf("[Poller] ✅ %d entri baru dari audit_trail (id > %d)", len(entries), lastID)
	return entries, nil
}

func (p *Poller) getSourceSystem(tabel string) string {
	for _, t := range p.cfg.Tables {
		if t.Name == tabel {
			return t.SourceSystem
		}
	}
	return "SIMRS-Unknown"
}
