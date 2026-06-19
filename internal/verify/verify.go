package verify

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ResourceRecord adalah data yang dikembalikan ke Gateway saat verifikasi.
// Berisi semua kolom non-geometry dari baris yang diminta.
type ResourceRecord struct {
	Found     bool                   `json:"found"`
	Table     string                 `json:"table"`
	ID        string                 `json:"id"`
	Data      map[string]interface{} `json:"data"`
	CheckedAt time.Time              `json:"checked_at"`
}

type Server struct {
	db          *pgxpool.Pool
	verifyToken string
	port        string
}

func NewServer(db *pgxpool.Pool, verifyToken, port string) *Server {
	return &Server{db: db, verifyToken: verifyToken, port: port}
}

func (s *Server) Start() {
	mux := http.NewServeMux()

	// Endpoint lama — audit_trail (untuk SIMRS)
	mux.HandleFunc("/verify/", s.handleVerify)

	// Endpoint baru — resource geospasial (untuk Satu Peta)
	// Format: GET /verify-resource/<table>/<id>
	mux.HandleFunc("/verify-resource/", s.handleVerifyResource)

	mux.HandleFunc("/health", s.handleHealth)

	addr := ":" + s.port
	log.Printf("🔍 [VerifyServer] Mendengarkan di %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("❌ [VerifyServer] Gagal start: %v", err)
	}
}

// handleVerifyResource melayani GET /verify-resource/<table>/<id>
// Gateway memanggil ini dengan nama tabel dan id dari kolom resource (format: tabel:id)
func (s *Server) handleVerifyResource(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Autentikasi
	if s.verifyToken != "" {
		got := r.Header.Get("Authorization")
		if got != "Bearer "+s.verifyToken {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}

	// Ekstrak table dan id dari path: /verify-resource/<table>/<id>
	path := strings.TrimPrefix(r.URL.Path, "/verify-resource/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		http.Error(w, "format path harus /verify-resource/<table>/<id>", http.StatusBadRequest)
		return
	}

	tableName := parts[0]
	resourceID := parts[1]

	rec := s.queryResource(r.Context(), tableName, resourceID)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(rec)
}

// queryResource mengambil satu baris dari tabel berdasarkan primary key
// Primary key dicoba secara berurutan: ogc_fid, id, _id, fid, gid
func (s *Server) queryResource(ctx context.Context, tableName, resourceID string) ResourceRecord {
	// Kandidat nama kolom primary key
	pkCandidates := []string{"ogc_fid", "id", "_id", "fid", "gid", "objectid"}

	// Cari kolom PK yang ada di tabel ini
	pkCol := s.findPKColumn(ctx, tableName, pkCandidates)
	if pkCol == "" {
		log.Printf("[VerifyServer] Tidak ditemukan kolom PK di tabel %s", tableName)
		return ResourceRecord{Found: false, Table: tableName, ID: resourceID, CheckedAt: time.Now()}
	}

	// Query baris — exclude kolom geometry otomatis via pg_catalog
	// Ambil semua kolom kecuali tipe geometry/geography
	query := `
		SELECT column_name 
		FROM information_schema.columns 
		WHERE table_schema = 'public' 
		AND table_name = $1
		AND udt_name NOT IN ('geometry', 'geography', 'raster')
		ORDER BY ordinal_position
	`

	rows, err := s.db.Query(ctx, query, tableName)
	if err != nil {
		log.Printf("[VerifyServer] Gagal ambil kolom tabel %s: %v", tableName, err)
		return ResourceRecord{Found: false, Table: tableName, ID: resourceID, CheckedAt: time.Now()}
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err == nil {
			columns = append(columns, col)
		}
	}

	if len(columns) == 0 {
		return ResourceRecord{Found: false, Table: tableName, ID: resourceID, CheckedAt: time.Now()}
	}

	// Build SELECT query dengan kolom yang aman
	colList := ""
	for i, col := range columns {
		if i > 0 {
			colList += ", "
		}
		colList += `"` + col + `"`
	}

	dataQuery := `SELECT ` + colList + ` FROM public."` + tableName + `" WHERE "` + pkCol + `" = $1 LIMIT 1`

	dataRows, err := s.db.Query(ctx, dataQuery, resourceID)
	if err != nil {
		log.Printf("[VerifyServer] Gagal query tabel %s id=%s: %v", tableName, resourceID, err)
		return ResourceRecord{Found: false, Table: tableName, ID: resourceID, CheckedAt: time.Now()}
	}
	defer dataRows.Close()

	if !dataRows.Next() {
		// Baris tidak ditemukan — kemungkinan sudah di-DELETE
		log.Printf("[VerifyServer] Baris tidak ditemukan: tabel=%s id=%s", tableName, resourceID)
		return ResourceRecord{Found: false, Table: tableName, ID: resourceID, CheckedAt: time.Now()}
	}

	// Scan hasil ke map
	fieldDescriptions := dataRows.FieldDescriptions()
	values, err := dataRows.Values()
	if err != nil {
		log.Printf("[VerifyServer] Gagal scan values: %v", err)
		return ResourceRecord{Found: false, Table: tableName, ID: resourceID, CheckedAt: time.Now()}
	}

	data := make(map[string]interface{})
	for i, fd := range fieldDescriptions {
		data[string(fd.Name)] = values[i]
	}

	return ResourceRecord{
		Found:     true,
		Table:     tableName,
		ID:        resourceID,
		Data:      data,
		CheckedAt: time.Now(),
	}
}

// findPKColumn mencari kolom primary key yang ada di tabel
func (s *Server) findPKColumn(ctx context.Context, tableName string, candidates []string) string {
	query := `
		SELECT column_name 
		FROM information_schema.columns 
		WHERE table_schema = 'public' 
		AND table_name = $1 
		AND column_name = ANY($2)
		LIMIT 1
	`

	var colName string
	for _, candidate := range candidates {
		err := s.db.QueryRow(ctx, query, tableName, []string{candidate}).Scan(&colName)
		if err == nil {
			return colName
		}
	}
	return ""
}

// handleVerify — endpoint lama untuk SIMRS (audit_trail by id)
func (s *Server) handleVerify(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if s.verifyToken != "" {
		got := r.Header.Get("Authorization")
		if got != "Bearer "+s.verifyToken {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}

	auditTrailID := r.URL.Path[len("/verify/"):]
	if auditTrailID == "" {
		http.Error(w, "audit_trail_id diperlukan", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"found":   false,
		"message": "endpoint ini tidak digunakan untuk Satu Peta",
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}
