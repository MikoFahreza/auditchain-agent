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

type TableConfig struct {
	Name          string `yaml:"name"`
	ActorField    string `yaml:"actor_field"`
	ResourceField string `yaml:"resource_field"`
	SourceSystem  string `yaml:"source_system"`
}

func Load(configPath string) (*Config, error) {
	// 1. Load .env
	godotenv.Load()

	// 2. Load tabel dari config.yml
	f, err := os.Open(configPath)
	if err != nil {
		return nil, fmt.Errorf("gagal buka config file: %w", err)
	}
	defer f.Close()

	var cfg Config
	if err := yaml.NewDecoder(f).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("gagal parse config file: %w", err)
	}

	// 3. Load nilai penting dari environment variables
	cfg.Client.APIKey = requireEnv("AUDITCHAIN_API_KEY")

	cfg.SourceDB.Host = requireEnv("DB_HOST")
	cfg.SourceDB.Port = envInt("DB_PORT", 5432)
	cfg.SourceDB.User = requireEnv("DB_USER")
	cfg.SourceDB.Password = requireEnv("DB_PASSWORD")
	cfg.SourceDB.DBName = requireEnv("DB_NAME")

	cfg.Gateway.URL = requireEnv("GATEWAY_URL")
	cfg.Gateway.TimeoutSeconds = envInt("GATEWAY_TIMEOUT_SECONDS", 10)

	cfg.Polling.IntervalSeconds = envInt("POLLING_INTERVAL_SECONDS", 5)
	cfg.Polling.BatchSize = envInt("POLLING_BATCH_SIZE", 50)

	return &cfg, nil
}

func requireEnv(key string) string {
	val := os.Getenv(key)
	if val == "" {
		panic(fmt.Sprintf("environment variable %s wajib diisi", key))
	}
	return val
}

func envInt(key string, defaultVal int) int {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	n, err := strconv.Atoi(val)
	if err != nil {
		return defaultVal
	}
	return n
}
