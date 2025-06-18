package cortexanalystapp

import (
	"context"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync/atomic"
	"time"
)

type api struct {
	snowflakeAccount   string
	snowflakeWarehouse string
	cfg                config
	currJWT            atomic.Pointer[string]
	mux                *http.ServeMux
}

func newAPI(ctx context.Context, privKey *rsa.PrivateKey, cfg config) (*api, error) {
	a := &api{
		mux:                http.NewServeMux(),
		cfg:                cfg,
		snowflakeAccount:   os.Getenv("SNOWFLAKE_ACCOUNT"),
		snowflakeWarehouse: os.Getenv("SNOWFLAKE_WAREHOUSE"),
	}
	if a.snowflakeAccount == "" || a.snowflakeWarehouse == "" {
		return nil, errors.New("missing environment variables SNOWFLAKE_ACCOUNT and SNOWFLAKE_WAREHOUSE")
	}
	initialJWT, err := generateJWT(privKey)
	if err != nil {
		return nil, err
	}
	a.currJWT.Store(&initialJWT)
	GoLoopEvery(ctx, 30*time.Minute, func() error {
		jwt, err := generateJWT(privKey)
		if err != nil {
			return err
		}
		a.currJWT.Store(&jwt)
		return nil
	})
	a.mux.HandleFunc("/api/models/", a.modelsHandler)
	a.mux.HandleFunc("/api/message/", a.messageHandler)
	return a, nil
}

type reqBody struct {
	Name     string    `json:"name"`
	Messages []message `json:"messages"`
}

type message struct {
	Role    string           `json:"role"`
	Content []messageContent `json:"content"`
}

type messageContent struct {
	Text        string   `json:"text"`
	Statement   string   `json:"statement"`
	Suggestions []string `json:"suggestions"`
	Type        string   `json:"type"`
}

type sendMessageRequest struct {
	SemanticModelFile string    `json:"semantic_model_file"`
	Stream            bool      `json:"stream"`
	Messages          []message `json:"messages"`
}

func (a *api) modelsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET requests are supported", http.StatusMethodNotAllowed)
		return
	}
	b, err := json.Marshal(a.cfg)
	if err != nil {
		http.Error(w, "Error marshalling response", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(b)
}

func (a *api) messageHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		http.Error(w, "Only GET/POST requests are supported", http.StatusMethodNotAllowed)
		return
	}
	var req reqBody
	if r.Method == http.MethodGet {
		reqEncoded := r.URL.Query().Get("req")
		if reqEncoded == "" {
			http.Error(w, "Missing req parameter", http.StatusBadRequest)
			return
		}
		reqDecoded, err := url.QueryUnescape(reqEncoded)
		if err != nil {
			log.Printf("Error decoding request parameter: %v", err)
			http.Error(w, "Error decoding request parameter", http.StatusBadRequest)
			return
		}
		if err := json.Unmarshal([]byte(reqDecoded), &req); err != nil {
			log.Printf("Error decoding request parameter: %v", err)
			http.Error(w, "Error decoding request parameter", http.StatusBadRequest)
			return
		}
	} else if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("Error decoding request body: %v", err)
		http.Error(w, "Error decoding request body", http.StatusBadRequest)
		return
	}
	var model *modelConfig
	for _, m := range a.cfg.Models {
		if m.Name == req.Name {
			model = &m
			break
		}
	}
	if model == nil {
		http.Error(w, fmt.Errorf("Unknown model: %s", req.Name).Error(), http.StatusNotFound)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	// SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	sqlGenerated := make(chan string, 1)
	textToSQLDone := make(chan struct{})
	go func() {
		defer close(textToSQLDone)
		a.handleTextToSQL(model, req.Messages, w, r, flusher, sqlGenerated)
	}()
	sqlExecuting := make(chan struct{})
	sqlExecuted := make(chan struct{})
forloop:
	for {
		select {
		case <-r.Context().Done():
			return
		case sql := <-sqlGenerated:
			close(sqlExecuting)
			go func() {
				defer close(sqlExecuted)
				a.execSQL(r.Context(), sql, w, r, flusher)
			}()
		case <-textToSQLDone:
			select {
			case <-sqlExecuting:
				// Wait for the SQL to be executed
				break forloop
			default:
				// No SQL was generated, so send the done event
				sendEvent(w, flusher, "status", statusData{Status: "done"})
				return
			}
		}
	}
	select {
	case <-sqlExecuted:
		sendEvent(w, flusher, "status", statusData{Status: "done"})
	case <-r.Context().Done():
	}
}

func (a *api) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*") // In production, replace * with your domain
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
	w.Header().Set("Access-Control-Max-Age", "86400") // 24 hours

	// Handle preflight requests
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}
	// Remove port from r.RemoteAddr
	host, _, _ := net.SplitHostPort(r.RemoteAddr)
	log.Printf("Request at path %s from address %s (User-Agent: %s)", r.URL.Path, host, r.UserAgent())
	a.mux.ServeHTTP(w, r)
}
