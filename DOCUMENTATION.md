# Dokumentasi Migrasi & API Agent (Oracle Database & SIMRS Morbis)

Dokumentasi ini menjelaskan pembaruan arsitektur pada **AuditChain Gateway Agent** dari PostgreSQL ke **Oracle Database** untuk kebutuhan integrasi verifikasi data pada sistem **SIMRS Morbis** dan **Satu Peta**.

---

## 1. Ringkasan Migrasi Database (PostgreSQL ke Oracle)

Agent saat ini dikonfigurasi dalam mode **Verify-Only** (sebagai server verifikasi data Lapis 3). Koneksi database telah sepenuhnya dimigrasikan dari PostgreSQL ke Oracle Database dengan rincian sebagai berikut:

### Teknologi & Driver
* **Driver**: Menggunakan `github.com/sijms/go-ora/v2` yang merupakan driver murni (*pure Go*) Oracle. Tidak membutuhkan instalasi Oracle Instant Client (OCI) atau CGO, sehingga sangat ringan dan portabel.
* **Koneksi via SID**: Alamat koneksi di-build secara otomatis menggunakan opsi **SID** (Service Identifier) Oracle (dalam kasus ini: `orclcdb`), bukan Service Name, untuk memastikan kecocokan dengan konfigurasi listener database SIMRS Morbis.

### Optimasi Query Dialek Oracle
1. **Resolusi Schema Owner Dinamis**:
   Pengecekan eksistensi tabel dilakukan dengan mencari pada tabel sistem `all_tables`. Pencarian memprioritaskan skema user aktif saat ini (`USER`), dan otomatis melakukan fallback ke skema lain jika tabel (seperti `BENTUK_MAKANAN`) dimiliki oleh skema yang berbeda (misalnya skema `SIMRS`).
2. **Deteksi Primary Key Constraint Otomatis**:
   Pencarian primary key tidak lagi bergantung pada daftar kandidat statis (`id`, `fid`, dll.). Agent akan menanyakan katalog constraint Oracle (`all_constraints` dan `all_cons_columns`) untuk mendeteksi kolom PK asli yang didefinisikan secara resmi pada tabel tersebut (contoh: `KODE_BENTUK_MAKANAN`).
3. **Penyaringan Tipe Data (Exclusion)**:
   Penyaringan otomatis kolom spasial (`SDO_GEOMETRY`) dan kolom binary besar (`BLOB`, `CLOB`, `RAW`, `LONG`) guna mempercepat query dan menjaga performa pengiriman data.
4. **Paging Limit**:
   Pembatasan pencarian baris menggunakan dialek khas Oracle (`ROWNUM <= 1`) menggantikan keyword PostgreSQL (`LIMIT 1`).

---

## 2. Struktur API Verifikasi (Inbound)

Semua endpoint verifikasi berjalan pada port verify yang ditentukan (default: `9090`) dan memerlukan autentikasi token Bearer.

### A. Endpoint Verifikasi SIMRS (`/verify/<table>/<id>`)
Endpoint ini digunakan oleh AuditChain Gateway untuk mengambil data non-spasial asli langsung dari database klien (SIMRS Morbis).

* **Method**: `GET`
* **URL**: `http://<agent-ip>:<port>/verify/{nama_tabel}/{id_data}`
* **Headers**:
  ```http
  Authorization: Bearer <AGENT_VERIFY_TOKEN>
  ```
* **Contoh Request**:
  `GET http://localhost:9090/verify/BENTUK_MAKANAN/1`

* **Contoh Response Payload (Data Ditemukan - `200 OK`)**:
  ```json
  {
    "found": true,
    "table": "BENTUK_MAKANAN",
    "id": "1",
    "data": {
      "KODE_BENTUK_MAKANAN": "1",
      "NAMA_BENTUK": "Cair",
      "KETERANGAN": "Bentuk makanan berupa cairan/bubur halus"
    },
    "checked_at": "2026-07-10T10:05:00.123456Z"
  }
  ```

* **Contoh Response Payload (Data Tidak Ditemukan - `200 OK`)**:
  ```json
  {
    "found": false,
    "table": "BENTUK_MAKANAN",
    "id": "999",
    "data": null,
    "checked_at": "2026-07-10T10:05:30.987654Z"
  }
  ```

---

### B. Endpoint Verifikasi Satu Peta Geospatial (`/verify-resource/<table>/<id>`)
Endpoint ini digunakan untuk memverifikasi data spasial (Satu Peta) dengan cara kerja yang serupa dengan endpoint SIMRS.

* **Method**: `GET`
* **URL**: `http://<agent-ip>:<port>/verify-resource/{nama_tabel}/{id_data}`
* **Headers**:
  ```http
  Authorization: Bearer <AGENT_VERIFY_TOKEN>
  ```

---

### C. Endpoint Health Check (`/health`)
Digunakan untuk memonitor status keaktifan server verify agent.

* **Method**: `GET`
* **URL**: `http://<agent-ip>:<port>/health`
* **Response Payload (`200 OK`)**:
  ```json
  {
    "status": "ok"
  }
  ```

---

## 3. Konfigurasi Environment (`.env`)

Sesuaikan variabel lingkungan pada berkas `.env` di root direktori Agent dengan kredensial Oracle SIMRS Morbis Anda:

```env
DB_HOST=192.168.60.65       # IP Database Server
DB_PORT=1526                # Port Listener Oracle
DB_USER=polinema            # Username DB Oracle
DB_PASSWORD=polinema        # Password DB Oracle
DB_NAME=orclcdb             # SID Oracle Database

# Token Autentikasi Keamanan (Harus sama dengan config di Gateway)
AGENT_VERIFY_TOKEN=rahasia123
AGENT_VERIFY_PORT=9090
```

---

## 4. Cara Menjalankan

Lakukan instalasi ulang dependency dan jalankan aplikasi Agent:

```bash
# 1. Install & tidy dependency Go
go mod tidy

# 2. Jalankan Server Agent
go run main.go
```
