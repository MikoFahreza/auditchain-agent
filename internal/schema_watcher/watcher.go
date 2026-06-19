package schemawatcher

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type SchemaChangePayload struct {
	Event     string    `json:"event"`
	Table     string    `json:"table"`
	Schema    string    `json:"schema"`
	Timestamp time.Time `json:"timestamp"`
}

type Watcher struct {
	db            *pgxpool.Pool
	debeziumURL   string
	connectorName string
}

func New(db *pgxpool.Pool, debeziumURL, connectorName string) *Watcher {
	return &Watcher{
		db:            db,
		debeziumURL:   debeziumURL,
		connectorName: connectorName,
	}
}

// Start mendengarkan notifikasi DDL dari PostgreSQL via LISTEN/NOTIFY
func (w *Watcher) Start(ctx context.Context) {
	log.Println("👁️  [SchemaWatcher] Mendengarkan perubahan DDL (CREATE/ALTER/DROP TABLE)...")

	conn, err := w.db.Acquire(ctx)
	if err != nil {
		log.Printf("❌ [SchemaWatcher] Gagal acquire koneksi: %v", err)
		return
	}
	defer conn.Release()

	// Subscribe ke channel schema_change
	if _, err := conn.Exec(ctx, "LISTEN schema_change"); err != nil {
		log.Printf("❌ [SchemaWatcher] Gagal LISTEN: %v", err)
		return
	}

	log.Println("✅ [SchemaWatcher] Siap mendeteksi CREATE/ALTER/DROP TABLE")

	for {
		select {
		case <-ctx.Done():
			log.Println("🛑 [SchemaWatcher] Berhenti.")
			return
		default:
			notification, err := conn.Conn().WaitForNotification(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("⚠️  [SchemaWatcher] Error menunggu notifikasi: %v", err)
				time.Sleep(2 * time.Second)
				continue
			}

			var payload SchemaChangePayload
			if err := json.Unmarshal([]byte(notification.Payload), &payload); err != nil {
				log.Printf("⚠️  [SchemaWatcher] Gagal parse payload: %v", err)
				continue
			}

			log.Printf("🔔 [SchemaWatcher] DDL terdeteksi: %s pada tabel %s",
				payload.Event, payload.Table)

			// Tunggu sebentar agar PostgreSQL selesai commit DDL
			time.Sleep(2 * time.Second)

			// Restart Debezium task agar mendeteksi tabel baru
			w.restartDebeziumTask()
		}
	}
}

// restartDebeziumTask memanggil REST API Debezium untuk restart task
func (w *Watcher) restartDebeziumTask() {
	url := fmt.Sprintf("%s/connectors/%s/tasks/0/restart",
		w.debeziumURL, w.connectorName)

	resp, err := http.Post(url, "application/json", nil)
	if err != nil {
		log.Printf("❌ [SchemaWatcher] Gagal restart Debezium task: %v", err)
		return
	}
	defer resp.Body.Close()

	log.Printf("✅ [SchemaWatcher] Debezium task di-restart (status: %d)", resp.StatusCode)
}
