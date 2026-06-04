package publisher

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"auditchain-agent/internal/config"
	"auditchain-agent/internal/poller"
)

// GatewayPayload adalah format raw yang dikirim ke Gateway.
// Field-field ini menggunakan nama kolom asli dari audit_trail —
// Gateway akan memetakannya ke field standar menggunakan ClientFieldMapping.
type GatewayPayload struct {
	// Field raw dari audit_trail (nama kolom asli)
	DBUser    string  `json:"db_user"`
	AppUser   *string `json:"app_user"`
	Tabel     string  `json:"tabel"`
	Operasi   string  `json:"operasi"`
	Timestamp string  `json:"timestamp"`

	// Metadata gabungan data_lama + data_baru untuk keperluan audit
	Metadata     map[string]interface{} `json:"metadata"`
	SourceSystem string                 `json:"source_system"`
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

// Publish mengirim batch log mentah ke Gateway API
func (p *Publisher) Publish(ctx context.Context, entries []poller.RawLogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	payloads := make([]GatewayPayload, 0, len(entries))
	for _, e := range entries {
		// Gabungkan data_lama dan data_baru ke dalam metadata
		metadata := map[string]interface{}{}
		if e.DataLama != nil {
			metadata["data_lama"] = e.DataLama
		}
		if e.DataBaru != nil {
			metadata["data_baru"] = e.DataBaru
		}

		payloads = append(payloads, GatewayPayload{
			DBUser:       e.DBUser,
			AppUser:      e.AppUser,
			Tabel:        e.Tabel,
			Operasi:      e.Operasi,
			Timestamp:    e.Waktu.Format(time.RFC3339),
			Metadata:     metadata,
			SourceSystem: e.SourceSystem,
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
		return fmt.Errorf("Gateway menolak request dengan status: %d", resp.StatusCode)
	}

	return nil
}
