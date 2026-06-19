package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"auditchain-agent/internal/config"
	"auditchain-agent/internal/consumer"
	"auditchain-agent/internal/publisher"
	schemawatcher "auditchain-agent/internal/schema_watcher"
	"auditchain-agent/internal/verify"

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
	log.Printf("   Gateway     : %s", cfg.Gateway.URL)
	log.Printf("   Kafka       : %s", cfg.Kafka.Brokers)
	log.Printf("   Topic prefix: %s", cfg.Kafka.TopicPrefix)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Koneksi ke PostgreSQL (untuk SchemaWatcher dan VerifyServer)
	dsn := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		cfg.SourceDB.Host, cfg.SourceDB.Port,
		cfg.SourceDB.User, cfg.SourceDB.Password, cfg.SourceDB.DBName,
	)

	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("❌ Gagal koneksi ke database: %v", err)
	}
	defer db.Close()

	if err := db.Ping(ctx); err != nil {
		log.Fatalf("❌ Database tidak bisa dijangkau: %v", err)
	}
	log.Println("✅ Koneksi ke database berhasil")

	// Verify server untuk Lapis 3 AuditChain
	verifyToken := getEnv("AGENT_VERIFY_TOKEN", "")
	verifyPort := getEnv("AGENT_VERIFY_PORT", "9090")
	verifyServer := verify.NewServer(db, verifyToken, verifyPort)
	go verifyServer.Start()

	// Schema watcher — mendeteksi CREATE/ALTER/DROP TABLE
	debeziumURL := getEnv("DEBEZIUM_URL", "http://localhost:8083")
	connectorName := getEnv("DEBEZIUM_CONNECTOR_NAME", "satu-peta-connector")
	watcher := schemawatcher.New(db, debeziumURL, connectorName)
	go watcher.Start(ctx)

	// Publisher ke Gateway
	pub := publisher.New(cfg)

	// Kafka consumer — membaca event CDC dari Debezium
	cons := consumer.New(cfg, pub)

	log.Println("🚀 AuditChain Agent (Debezium mode) mulai berjalan...")

	if err := cons.Start(ctx); err != nil {
		log.Fatalf("❌ Consumer error: %v", err)
	}

	log.Println("✅ Agent berhenti dengan bersih.")
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
