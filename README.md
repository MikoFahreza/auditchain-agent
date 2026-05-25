# auditchain-agent

Event publisher agent untuk AuditChain Gateway. Dipasang di sisi klien untuk membaca perubahan database secara otomatis dan mengirimkannya ke AuditChain Gateway tanpa menyentuh kode aplikasi klien.

## Cara Kerja

```
[Database Klien] → polling setiap N detik → [Agent] → POST /api/v1/logs → [Gateway]
```

Agent membaca baris yang berubah berdasarkan kolom `modified_at`, lalu mengirimkan sebagai bulk log ke Gateway API.

## Prasyarat

- Go 1.24+
- Akses ke database sumber (PostgreSQL)
- API Key dari AuditChain Gateway

## Konfigurasi

Salin dan sesuaikan `config.yml`:

```yaml
client:
  api_key: "ak_live_xxxxxxxxxxxx"   # dari POST /api/admin/clients

source_db:
  host: "localhost"
  port: 5434
  user: "simrs"
  password: "simrs123"
  dbname: "simrs_db"

gateway:
  url: "http://192.168.11.94:8080"

polling:
  interval_seconds: 5
  batch_size: 50

tables:
  - name: pasien
    actor_field: modified_by
    resource_field: no_rm
    source_system: SIMRS-Pasien
```

## Menjalankan

```bash
# Install dependencies
go mod tidy

# Jalankan dengan config default (config.yml)
go run main.go

# Atau tentukan path config sendiri
go run main.go /path/to/config.yml
```

## Build Binary

```bash
go build -o auditchain-agent main.go
./auditchain-agent
```

## Syarat Database Sumber

Tabel yang dimonitor harus memiliki kolom `modified_at TIMESTAMPTZ` yang terupdate otomatis setiap ada perubahan.

Database juga harus memiliki tabel `agent_checkpoints` untuk tracking posisi polling — sudah dibuat otomatis oleh `init.sql` di repo `auditchain-simrs-dummy`.