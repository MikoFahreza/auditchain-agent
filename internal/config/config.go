package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Client   ClientConfig   `yaml:"client"`
	SourceDB SourceDBConfig `yaml:"source_db"`
	Gateway  GatewayConfig  `yaml:"gateway"`
	Polling  PollingConfig  `yaml:"polling"`
	Tables   []TableConfig  `yaml:"tables"`
}

type ClientConfig struct {
	APIKey string `yaml:"api_key"`
}

type SourceDBConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	DBName   string `yaml:"dbname"`
}

type GatewayConfig struct {
	URL            string `yaml:"url"`
	TimeoutSeconds int    `yaml:"timeout_seconds"`
}

type PollingConfig struct {
	IntervalSeconds int `yaml:"interval_seconds"`
	BatchSize       int `yaml:"batch_size"`
}

type TableConfig struct {
	Name          string `yaml:"name"`
	ActorField    string `yaml:"actor_field"`
	ResourceField string `yaml:"resource_field"`
	SourceSystem  string `yaml:"source_system"`
}

func Load(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var cfg Config
	if err := yaml.NewDecoder(f).Decode(&cfg); err != nil {
		return nil, err
	}

	// Default values
	if cfg.Polling.IntervalSeconds == 0 {
		cfg.Polling.IntervalSeconds = 5
	}
	if cfg.Polling.BatchSize == 0 {
		cfg.Polling.BatchSize = 50
	}
	if cfg.Gateway.TimeoutSeconds == 0 {
		cfg.Gateway.TimeoutSeconds = 10
	}

	return &cfg, nil
}
