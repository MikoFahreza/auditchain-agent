package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"auditchain-agent/internal/config"
	"auditchain-agent/internal/publisher"

	"github.com/segmentio/kafka-go"
)

// DebeziumMessage adalah struktur message dari Kafka topic Debezium
// dengan konfigurasi unwrap ExtractNewRecordState
type DebeziumMessage struct {
	Payload map[string]interface{} `json:"payload"`
}

type Consumer struct {
	cfg       *config.Config
	publisher *publisher.Publisher
}

func New(cfg *config.Config, pub *publisher.Publisher) *Consumer {
	return &Consumer{cfg: cfg, publisher: pub}
}

// Start membaca semua topic dengan prefix satu_peta.public.*
// dan meneruskan ke publisher
func (c *Consumer) Start(ctx context.Context) error {
	log.Printf("🎧 [Consumer] Menghubungi Kafka di %s...", c.cfg.Kafka.Brokers)

	// Discover semua topic yang tersedia
	conn, err := kafka.Dial("tcp", c.cfg.Kafka.Brokers)
	if err != nil {
		return fmt.Errorf("gagal koneksi ke Kafka: %w", err)
	}

	partitions, err := conn.ReadPartitions()
	conn.Close()
	if err != nil {
		return fmt.Errorf("gagal baca partisi Kafka: %w", err)
	}

	// Filter topic dengan prefix satu_peta.public.
	topicSet := make(map[string]struct{})
	for _, p := range partitions {
		if strings.HasPrefix(p.Topic, c.cfg.Kafka.TopicPrefix) {
			topicSet[p.Topic] = struct{}{}
		}
	}

	if len(topicSet) == 0 {
		log.Printf("⚠️  [Consumer] Belum ada topic dengan prefix %s", c.cfg.Kafka.TopicPrefix)
	}

	topics := make([]string, 0, len(topicSet))
	for t := range topicSet {
		topics = append(topics, t)
	}

	log.Printf("📋 [Consumer] Ditemukan %d topic, mulai consume...", len(topics))

	// Buat reader untuk semua topic sekaligus
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{c.cfg.Kafka.Brokers},
		GroupID:        c.cfg.Kafka.GroupID,
		GroupTopics:    topics,
		MinBytes:       1,
		MaxBytes:       10e6, // 10MB
		CommitInterval: time.Second,
		StartOffset:    kafka.LastOffset, // hanya baca message baru
	})
	defer reader.Close()

	log.Println("✅ [Consumer] Siap menerima event perubahan data...")

	for {
		select {
		case <-ctx.Done():
			log.Println("🛑 [Consumer] Berhenti.")
			return nil
		default:
			msg, err := reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				log.Printf("⚠️  [Consumer] Gagal fetch message: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}

			if err := c.processMessage(msg); err != nil {
				log.Printf("⚠️  [Consumer] Gagal proses message topic=%s: %v", msg.Topic, err)
			}

			// Commit offset setelah berhasil diproses
			if err := reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("⚠️  [Consumer] Gagal commit offset: %v", err)
			}
		}
	}
}

// processMessage mengurai message Debezium dan meneruskan ke publisher
func (c *Consumer) processMessage(msg kafka.Message) error {
	var debMsg DebeziumMessage
	if err := json.Unmarshal(msg.Value, &debMsg); err != nil {
		return fmt.Errorf("gagal parse JSON: %w", err)
	}

	payload := debMsg.Payload
	if payload == nil {
		return nil
	}

	// Ambil field metadata dari payload
	op, _ := payload["__op"].(string)
	table, _ := payload["__table"].(string)
	tsMs, _ := payload["__ts_ms"].(float64)

	if table == "" {
		return nil
	}

	// Skip event snapshot (op="r") — hanya proses perubahan nyata
	if op == "r" {
		return nil
	}

	// Konversi op Debezium ke action standar
	action := opToAction(op)

	// Cari primary key untuk resource ID
	resourceID := findPrimaryKey(payload)

	// Format resource: "nama_tabel:id"
	resource := fmt.Sprintf("%s:%s", table, resourceID)

	// Konversi timestamp
	var timestamp time.Time
	if tsMs > 0 {
		timestamp = time.UnixMilli(int64(tsMs))
	} else {
		timestamp = time.Now()
	}

	// Buat metadata dari semua field non-sistem
	metadata := extractMetadata(payload)

	entry := publisher.LogEntry{
		Actor:        "satu-peta-system",
		Action:       action,
		Resource:     resource,
		Timestamp:    timestamp,
		SourceSystem: c.cfg.Kafka.SourceSystem,
		Metadata:     metadata,
	}

	log.Printf("📨 [Consumer] %s %s %s", action, table, resourceID)

	return c.publisher.Publish(context.Background(), []publisher.LogEntry{entry})
}

// opToAction mengkonversi kode operasi Debezium ke action standar
func opToAction(op string) string {
	switch op {
	case "c":
		return "INSERT"
	case "u":
		return "UPDATE"
	case "d":
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}

// findPrimaryKey mencari nilai primary key dari payload
// Prioritas: ogc_fid → id → _id → fid → gid
func findPrimaryKey(payload map[string]interface{}) string {
	candidates := []string{"ogc_fid", "id", "_id", "fid", "gid", "objectid"}
	for _, key := range candidates {
		if val, ok := payload[key]; ok && val != nil {
			return fmt.Sprintf("%v", val)
		}
	}
	return "unknown"
}

// extractMetadata mengambil semua field non-sistem sebagai metadata
func extractMetadata(payload map[string]interface{}) map[string]interface{} {
	skip := map[string]bool{
		"__op": true, "__table": true, "__db": true,
		"__ts_ms": true, "__deleted": true,
	}

	meta := make(map[string]interface{})
	for k, v := range payload {
		if !skip[k] {
			meta[k] = v
		}
	}
	return meta
}
