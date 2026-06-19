package publisher

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"auditchain-agent/internal/config"
)

type LogEntry struct {
	Actor        string
	Action       string
	Resource     string
	Timestamp    time.Time
	SourceSystem string
	Metadata     map[string]interface{}
}

type GatewayPayload struct {
	Actor        string                 `json:"actor"`
	Action       string                 `json:"action"`
	Resource     string                 `json:"resource"`
	Timestamp    string                 `json:"timestamp"`
	SourceSystem string                 `json:"source_system"`
	Metadata     map[string]interface{} `json:"metadata"`
}

type Publisher struct {
	cfg    *config.Config
	client *http.Client
}

func New(cfg *config.Config) *Publisher {
	return &Publisher{
		cfg: cfg,
		client: &http.Client{
			Timeout: time.Duration(cfg.Gateway.TimeoutSeconds) * time.Second,
		},
	}
}

func (p *Publisher) Publish(ctx context.Context, entries []LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	payloads := make([]GatewayPayload, 0, len(entries))
	for _, e := range entries {
		payloads = append(payloads, GatewayPayload{
			Actor:        e.Actor,
			Action:       e.Action,
			Resource:     e.Resource,
			Timestamp:    e.Timestamp.Format(time.RFC3339),
			SourceSystem: e.SourceSystem,
			Metadata:     e.Metadata,
		})
	}

	body, err := json.Marshal(payloads)
	if err != nil {
		return fmt.Errorf("gagal marshal payload: %w", err)
	}

	url := p.cfg.Gateway.URL + "/api/v1/logs/"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("gagal buat request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("api-key", p.cfg.Client.APIKey)

	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("gagal kirim ke Gateway: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("Gateway menolak dengan status: %d", resp.StatusCode)
	}

	return nil
}
