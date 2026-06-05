package verify

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// AuditTrailRecord adalah data yang dikembalikan ke Gateway saat verifikasi.
// Field disesuaikan persis dengan kolom tabel audit_trail di DB klien.
type AuditTrailRecord struct {
	Found    bool                   `json:"found"`
	ID       int                    `json:"id"`
	Tabel    string                 `json:"tabel"`
	Operasi  string                 `json:"operasi"`
	DBUser   string                 `json:"db_user"`
	AppUser  *string                `json:"app_user"`
	DataLama map[string]interface{} `json:"data_lama"`
	DataBaru map[string]interface{} `json:"data_baru"`
	Waktu    time.Time              `json:"waktu"`
}

// Server adalah HTTP server kecil yang berjalan di sisi Agent
// untuk melayani request verifikasi dari Gateway.
type Server struct {
	db          *pgxpool.Pool
	verifyToken string // Bearer token — harus sama dengan yang dikonfigurasi di Gateway
	port        string
}

func NewServer(db *pgxpool.Pool, verifyToken, port string) *Server {
	return &Server{db: db, verifyToken: verifyToken, port: port}
}

func (s *Server) Start() {
	mux := http.NewServeMux()
	mux.HandleFunc("/verify/", s.handleVerify)
	mux.HandleFunc("/health", s.handleHealth)

	addr := ":" + s.port
	log.Printf("🔍 [VerifyServer] Mendengarkan di %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("❌ [VerifyServer] Gagal start: %v", err)
	}
}

// handleVerify melayani GET /verify/<audit_trail_id>
// Gateway memanggil ini dengan ID dari kolom audit_trail.id yang
// sudah dikirim Agent ke Gateway sebelumnya sebagai log_id.
func (s *Server) handleVerify(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Autentikasi: pastikan request dari Gateway yang terdaftar
	if s.verifyToken != "" {
		got := r.Header.Get("Authorization")
		if got != "Bearer "+s.verifyToken {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}

	// Ekstrak audit_trail_id dari path: /verify/<id>
	auditTrailID := r.URL.Path[len("/verify/"):]
	if auditTrailID == "" {
		http.Error(w, "audit_trail_id diperlukan", http.StatusBadRequest)
		return
	}

	rec := s.queryAuditTrail(r.Context(), auditTrailID)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(rec)
}

// queryAuditTrail mengambil satu baris dari audit_trail berdasarkan ID.
// ID ini adalah nilai integer dari kolom `id` di audit_trail,
// yang dikirim Agent ke Gateway sebagai bagian dari log_id.
func (s *Server) queryAuditTrail(ctx context.Context, id string) AuditTrailRecord {
	var rec AuditTrailRecord
	var dataLamaRaw, dataBaruRaw *string

	err := s.db.QueryRow(ctx, `
		SELECT id, tabel, operasi, db_user, app_user, data_lama, data_baru, waktu
		FROM audit_trail
		WHERE id = $1
		LIMIT 1
	`, id).Scan(
		&rec.ID,
		&rec.Tabel,
		&rec.Operasi,
		&rec.DBUser,
		&rec.AppUser,
		&dataLamaRaw,
		&dataBaruRaw,
		&rec.Waktu,
	)

	if err != nil {
		// Tidak ditemukan atau error → kembalikan Found=false
		// Gateway akan menafsirkan ini sebagai indikasi penghapusan ilegal
		log.Printf("[VerifyServer] Tidak ditemukan audit_trail id=%s: %v", id, err)
		return AuditTrailRecord{Found: false}
	}

	if dataLamaRaw != nil {
		json.Unmarshal([]byte(*dataLamaRaw), &rec.DataLama)
	}
	if dataBaruRaw != nil {
		json.Unmarshal([]byte(*dataBaruRaw), &rec.DataBaru)
	}

	rec.Found = true
	return rec
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}
