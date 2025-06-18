package cortexanalystapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/tmaxmax/go-sse"
)

type statusData struct {
	Status        string `json:"status"`
	StatusMessage string `json:"status_message"`
}

type messageContentDeltaData struct {
	Type           string `json:"type"`
	TextDelta      string `json:"text_delta,omitempty"`
	StatementDelta string `json:"statement_delta,omitempty"`
	Index          int    `json:"index"`
}

func sendEvent(w http.ResponseWriter, flusher http.Flusher, eventType string, data any) {
	dataMarshalled, err := json.Marshal(data)
	if err != nil {
		log.Printf("Error marshalling data: %v", err)
		http.Error(w, "Error marshalling data", http.StatusInternalServerError)
		return
	}
	if eventType != "" {
		_, _ = fmt.Fprintf(w, "event: %s\n", eventType)
	}
	_, _ = fmt.Fprintf(w, "data: %s\n\n", string(dataMarshalled))
	flusher.Flush()
	log.Printf("Sent event: %s %s", eventType, string(dataMarshalled))
}

func (a *api) handleTextToSQL(model *modelConfig, messages []message, w http.ResponseWriter, r *http.Request, flusher http.Flusher, sqlGenerated chan<- string) {
	log.Printf("Handling text to SQL request")
	smr, err := json.Marshal(sendMessageRequest{
		SemanticModelFile: model.Path,
		Stream:            true,
		Messages:          messages,
	})
	if err != nil {
		log.Printf("Error marshalling request: %v", err)
		http.Error(w, "Error marshalling request", http.StatusInternalServerError)
		return
	}

	analystReq, err := http.NewRequestWithContext(r.Context(), http.MethodPost,
		fmt.Sprintf("https://%s.snowflakecomputing.com/api/v2/cortex/analyst/message", a.snowflakeAccount), bytes.NewReader(smr))
	if err != nil {
		log.Printf("Error creating request: %v", err)
		http.Error(w, "Error creating request", http.StatusInternalServerError)
		return
	}
	analystReq.Header.Set("Authorization", fmt.Sprintf("Bearer %s", *a.currJWT.Load()))
	analystReq.Header.Set("Content-Type", "application/json")
	analystReq.Header.Set("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
	analystReq.Header.Set("Accept", "text/event-stream")
	resp, err := http.DefaultClient.Do(analystReq)
	if err != nil {
		log.Printf("Error sending request to Analyst: %v", err)
		http.Error(w, "Error sending request", http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		log.Printf("Analyst returned status code %d (body %s)", resp.StatusCode, string(body))
		http.Error(w, fmt.Errorf("Analyst returned status code %d", resp.StatusCode).Error(), resp.StatusCode)
		return
	}
	contentType := resp.Header.Get("Content-Type")
	if contentType != "text/event-stream" {
		body, _ := io.ReadAll(resp.Body)
		log.Printf("Analyst returned unexpected Content-Type: %s (body: %s)", contentType, string(body))
		http.Error(w, "Analyst returned unexpected Content-Type", http.StatusInternalServerError)
	}
	for evt, err := range sse.Read(resp.Body, nil) {
		if err != nil {
			log.Printf("Error reading event: %v", err)
			break
		}
		// If event.Type is status and status is "done", close the done channel
		if evt.Type == "status" {
			var status statusData
			if err := json.Unmarshal([]byte(evt.Data), &status); err != nil {
				log.Printf("Error unmarshalling status: %v", err)
				http.Error(w, "Error unmarshalling status", http.StatusInternalServerError)
				return
			}
			if status.Status == "done" {
				return
			}
		}
		// Now check if it's a message.content.delta event that contains SQL
		if evt.Type == "message.content.delta" {
			var delta messageContentDeltaData
			if err := json.Unmarshal([]byte(evt.Data), &delta); err != nil {
				log.Printf("Error unmarshalling delta: %v", err)
				http.Error(w, "Error unmarshalling delta", http.StatusInternalServerError)
				return
			}
			if delta.Type == "sql" {
				// Unconditional channel send is ok, we create the channel with a buffer of 1
				sqlGenerated <- delta.StatementDelta
				// Fall through to sending the event
			}
		}
		if evt.Type != "" {
			_, _ = fmt.Fprintf(w, "event: %s\n", evt.Type)
		}
		fmt.Fprintf(w, "data: %s\n\n", evt.Data)
		flusher.Flush()
		jsonEvt, _ := json.MarshalIndent(evt, "", "  ")
		log.Printf("Sent event: %v", string(jsonEvt))
	}
}

func (a *api) execSQL(ctx context.Context, sql string, w http.ResponseWriter, r *http.Request, flusher http.Flusher) {
	updateCh := make(chan execSQLUpdate)
	errCh := make(chan error, 1)
	go func() {
		errCh <- execSnowflakeSQL(ctx, a.snowflakeAccount, *a.currJWT.Load(), StatementRequest{Statement: sql, Warehouse: a.snowflakeWarehouse}, updateCh)
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case err := <-errCh:
			if err != nil {
				log.Printf("Error executing SQL: %v", err)
			}
			return
		case update := <-updateCh:
			sendEvent(w, flusher, update.Type, update.Event)
		}
	}
}
