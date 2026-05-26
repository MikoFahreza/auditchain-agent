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
	Polling  PollingConfig
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

type PollingConfig struct {
	IntervalSeconds int
	BatchSize       int
}

// TableConfig hanya menyimpan source_system per tabel
// actor dan resource diambil langsung dari audit_trail via trigger
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
	cfg.SourceDB.Host = getEnv("DB_HOST", "localhost")
	cfg.SourceDB.Port = getEnvInt("DB_PORT", 5432)
	cfg.SourceDB.User = requireEnv("DB_USER")
	cfg.SourceDB.Password = requireEnv("DB_PASSWORD")
	cfg.SourceDB.DBName = requireEnv("DB_NAME")
	cfg.Gateway.URL = requireEnv("GATEWAY_URL")
	cfg.Gateway.TimeoutSeconds = getEnvInt("GATEWAY_TIMEOUT_SECONDS", 10)
	cfg.Polling.IntervalSeconds = getEnvInt("POLLING_INTERVAL_SECONDS", 5)
	cfg.Polling.BatchSize = getEnvInt("POLLING_BATCH_SIZE", 50)

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
