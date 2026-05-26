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

// LogEntry adalah hasil polling dari audit_trail
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

// Poll mengambil entri baru dari audit_trail sejak id terakhir
func (p *Poller) Poll(ctx context.Context) ([]LogEntry, error) {
	// 1. Ambil checkpoint terakhir
	var lastIDStr string
	err := p.db.QueryRow(ctx,
		"SELECT value FROM agent_checkpoints WHERE key = 'audit_trail_last_id'",
	).Scan(&lastIDStr)
	if err != nil {
		return nil, fmt.Errorf("gagal baca checkpoint: %w", err)
	}

	var lastID int
	fmt.Sscanf(lastIDStr, "%d", &lastID)

	// 2. Ambil entri baru dari audit_trail
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

	var entries []LogEntry
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

		// Tentukan actor: utamakan app_user, fallback ke db_user
		actor := dbUser
		if appUser != nil && *appUser != "" {
			actor = *appUser
		}

		// Tentukan resource dari data_baru atau data_lama
		resource := tabel
		dataJSON := dataBaruRaw
		if dataJSON == nil {
			dataJSON = dataLamaRaw
		}

		var dataMap map[string]interface{}
		if dataJSON != nil {
			json.Unmarshal([]byte(*dataJSON), &dataMap)
			resource = buildResource(tabel, dataMap)
		}

		// Tentukan source_system dari config tabel
		sourceSystem := p.getSourceSystem(tabel)

		// Metadata: gabung data_lama dan data_baru
		metadata := map[string]interface{}{
			"db_user":  dbUser,
			"app_user": appUser,
		}
		if dataLamaRaw != nil {
			var dl map[string]interface{}
			json.Unmarshal([]byte(*dataLamaRaw), &dl)
			metadata["data_lama"] = dl
		}
		if dataBaruRaw != nil {
			var db map[string]interface{}
			json.Unmarshal([]byte(*dataBaruRaw), &db)
			metadata["data_baru"] = db
		}

		entries = append(entries, LogEntry{
			Actor:        actor,
			Action:       operasi, // INSERT / UPDATE / DELETE
			Resource:     resource,
			Timestamp:    waktu,
			SourceSystem: sourceSystem,
			Metadata:     metadata,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(entries) == 0 {
		return nil, nil
	}

	// 3. Update checkpoint ke id terakhir yang sudah diambil
	_, err = p.db.Exec(ctx,
		"UPDATE agent_checkpoints SET value = $1 WHERE key = 'audit_trail_last_id'",
		fmt.Sprintf("%d", latestID),
	)
	if err != nil {
		return nil, fmt.Errorf("gagal update checkpoint: %w", err)
	}

	log.Printf("[Poller] ✅ %d entri baru dari audit_trail (id %d → %d)", len(entries), lastID+1, latestID)
	return entries, nil
}

// buildResource membentuk identifier unik dari data baris
func buildResource(tabel string, data map[string]interface{}) string {
	keys := map[string]string{
		"pasien":      "no_rm",
		"rekam_medis": "no_rm",
		"transaksi":   "no_invoice",
	}
	if key, ok := keys[tabel]; ok {
		if val, ok := data[key]; ok {
			return fmt.Sprintf("%s:%s:%v", tabel, key, val)
		}
	}
	if id, ok := data["id"]; ok {
		return fmt.Sprintf("%s:id:%v", tabel, id)
	}
	return tabel
}

// getSourceSystem mencari source_system dari config tabel
func (p *Poller) getSourceSystem(tabel string) string {
	for _, t := range p.cfg.Tables {
		if t.Name == tabel {
			return t.SourceSystem
		}
	}
	return "SIMRS-Unknown"
}
