package config

import (
	"fmt"
	"os"
	"strconv"

	"github.com/joho/godotenv"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Client   ClientConfig
	SourceDB SourceDBConfig
	Gateway  GatewayConfig
	Kafka    KafkaConfig
	Tables   []TableConfig `yaml:"tables"`
}

type ClientConfig struct {
	APIKey string
}

type SourceDBConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
}

type GatewayConfig struct {
	URL            string
	TimeoutSeconds int
}

type KafkaConfig struct {
	Brokers      string
	TopicPrefix  string
	GroupID      string
	SourceSystem string
}

type TableConfig struct {
	Name         string `yaml:"name"`
	SourceSystem string `yaml:"source_system"`
}

func Load(configPath string) (*Config, error) {
	godotenv.Load()

	f, err := os.Open(configPath)
	if err != nil {
		return nil, fmt.Errorf("gagal buka config file: %w", err)
	}
	defer f.Close()

	var cfg Config
	if err := yaml.NewDecoder(f).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("gagal parse config file: %w", err)
	}

	cfg.Client.APIKey = requireEnv("AUDITCHAIN_API_KEY")
	cfg.Gateway.URL = requireEnv("GATEWAY_URL")
	cfg.Gateway.TimeoutSeconds = getEnvInt("GATEWAY_TIMEOUT_SECONDS", 10)

	cfg.SourceDB.Host = getEnv("DB_HOST", "localhost")
	cfg.SourceDB.Port = getEnvInt("DB_PORT", 5432)
	cfg.SourceDB.User = getEnv("DB_USER", "")
	cfg.SourceDB.Password = getEnv("DB_PASSWORD", "")
	cfg.SourceDB.DBName = getEnv("DB_NAME", "test_postgis")

	cfg.Kafka.Brokers = getEnv("KAFKA_BROKERS", "localhost:9092")
	cfg.Kafka.TopicPrefix = getEnv("KAFKA_TOPIC_PREFIX", "satu_peta.public.")
	cfg.Kafka.GroupID = getEnv("KAFKA_GROUP_ID", "auditchain-agent-group")
	cfg.Kafka.SourceSystem = getEnv("KAFKA_SOURCE_SYSTEM", "SATU-PETA")

	return &cfg, nil
}

func requireEnv(key string) string {
	val := os.Getenv(key)
	if val == "" {
		panic(fmt.Sprintf("environment variable %s wajib diisi", key))
	}
	return val
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
