package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"auditchain-agent/internal/config"
	"auditchain-agent/internal/verify"

	goora "github.com/sijms/go-ora/v2"
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

	// Ekstrak host dan port dari config
	host := cfg.SourceDB.Host
	port := cfg.SourceDB.Port
	if strings.Contains(host, ":") {
		parts := strings.Split(host, ":")
		host = parts[0]
		if p, err := strconv.Atoi(parts[1]); err == nil {
			port = p
		}
	}

	// Gunakan BuildUrl dari go-ora dengan opsi SID
	urlOptions := map[string]string{
		"SID": cfg.SourceDB.DBName,
	}
	dsn := goora.BuildUrl(host, port, "", cfg.SourceDB.User, cfg.SourceDB.Password, urlOptions)

	db, err := sql.Open("oracle", dsn)
	if err != nil {
		log.Fatalf("❌ Gagal koneksi ke database: %v", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("❌ Database tidak bisa dijangkau: %v", err)
	}
	log.Println("✅ Koneksi ke database Oracle (SID) berhasil")

	// Verify server untuk Lapis 3 AuditChain
	verifyToken := getEnv("AGENT_VERIFY_TOKEN", "")
	verifyPort := getEnv("AGENT_VERIFY_PORT", "9090")
	verifyServer := verify.NewServer(db, verifyToken, verifyPort)
	go verifyServer.Start()

	log.Println("🚀 AuditChain Agent (Verify-Only mode) mulai berjalan...")

	// Tunggu sinyal interupsi/shutdown untuk berhenti secara bersih
	<-ctx.Done()

	log.Println("✅ Agent berhenti dengan bersih.")
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
