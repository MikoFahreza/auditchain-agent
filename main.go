package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"auditchain-agent/internal/config"
	"auditchain-agent/internal/poller"
	"auditchain-agent/internal/publisher"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	cfgPath := "config.yml"
	if len(os.Args) > 1 {
		cfgPath = os.Args[1]
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatalf("❌ Gagal load config: %v", err)
	}

	log.Println("✅ Konfigurasi berhasil dimuat")
	log.Printf("   Gateway  : %s", cfg.Gateway.URL)
	log.Printf("   Source DB: %s:%d/%s", cfg.SourceDB.Host, cfg.SourceDB.Port, cfg.SourceDB.DBName)
	log.Printf("   Interval : %d detik", cfg.Polling.IntervalSeconds)

	dsn := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		cfg.SourceDB.Host, cfg.SourceDB.Port,
		cfg.SourceDB.User, cfg.SourceDB.Password, cfg.SourceDB.DBName,
	)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("❌ Gagal koneksi ke database: %v", err)
	}
	defer db.Close()

	if err := db.Ping(ctx); err != nil {
		log.Fatalf("❌ Database tidak bisa dijangkau: %v", err)
	}
	log.Println("✅ Koneksi ke database berhasil")

	p := poller.New(db, cfg)
	pub := publisher.New(cfg)

	log.Println("🚀 AuditChain Agent mulai berjalan — polling audit_trail...")
	ticker := time.NewTicker(time.Duration(cfg.Polling.IntervalSeconds) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("🛑 Agent berhenti dengan bersih.")
			return

		case <-ticker.C:
			entries, err := p.Poll(ctx)
			if err != nil {
				log.Printf("❌ [Poller] Error: %v", err)
				continue
			}

			if len(entries) == 0 {
				continue
			}

			if err := pub.Publish(ctx, entries); err != nil {
				log.Printf("❌ [Publisher] Gagal kirim %d log: %v", len(entries), err)
				continue
			}

			log.Printf("📤 [Publisher] %d log berhasil dikirim ke Gateway", len(entries))
		}
	}
}
