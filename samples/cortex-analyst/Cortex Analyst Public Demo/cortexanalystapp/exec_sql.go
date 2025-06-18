package cortexanalystapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Parameters struct {
	BinaryOutputFormat       string `json:"binary_output_format,omitempty"`
	ClientResultChunkSize    int    `json:"client_result_chunk_size,omitempty"`
	DateOutputFormat         string `json:"date_output_format,omitempty"`
	MultiStatementCount      string `json:"multi_statement_count,omitempty"`
	QueryTag                 string `json:"query_tag,omitempty"`
	RowsPerResultSet         int    `json:"rows_per_resultset,omitempty"`
	TimeOutputFormat         string `json:"time_output_format,omitempty"`
	TimestampLTZOutputFormat string `json:"timestamp_ltz_output_format,omitempty"`
	TimestampNTZOutputFormat string `json:"timestamp_ntz_output_format,omitempty"`
	TimestampOutputFormat    string `json:"timestamp_output_format,omitempty"`
	TimestampTZOutputFormat  string `json:"timestamp_tz_output_format,omitempty"`
	Timezone                 string `json:"timezone,omitempty"`
	UseCachedResult          string `json:"use_cached_result,omitempty"`
}

// StatementRequest defines the JSON body to send to the Snowflake SQL API.
type StatementRequest struct {
	Statement  string     `json:"statement"`
	Timeout    int        `json:"timeout,omitempty"`
	Database   string     `json:"database,omitempty"`
	Schema     string     `json:"schema,omitempty"`
	Warehouse  string     `json:"warehouse,omitempty"`
	Role       string     `json:"role,omitempty"`
	Parameters Parameters `json:"parameters,omitempty"`
}

// QueryStatus represents a query status response from the Snowflake SQL API.
type QueryStatus struct {
	Code               string `json:"code"`
	SQLState           string `json:"sqlState"`
	Message            string `json:"message"`
	StatementHandle    string `json:"statementHandle"`
	StatementStatusURL string `json:"statementStatusUrl"`
	CreatedOnMillis    int64  `json:"createdOn"`
}

// ResultSet represents a query result set response from the Snowflake SQL API.
type ResultSet struct {
	Code               string            `json:"code"`
	SQLState           string            `json:"sqlState"`
	Message            string            `json:"message"`
	StatementHandle    string            `json:"statementHandle"`
	StatementHandles   []string          `json:"statementHandles"`
	CreatedOn          int64             `json:"createdOn"`
	StatementStatusURL string            `json:"statementStatusUrl"`
	ResultSetMetaData  ResultSetMetaData `json:"resultSetMetaData"` // Define the appropriate struct for metadata
	Data               [][]string        `json:"data"`
}

type ResultSetMetaData struct {
	Partition int64     `json:"partition"`
	NumRows   int64     `json:"numRows"`
	Format    string    `json:"format"`
	RowType   []RowType `json:"rowType"`
}

type PartitionInfo struct {
	RowCount         int64           `json:"rowCount"`
	UncompressedSize int64           `json:"uncompressedSize"`
	CompressedSize   int64           `json:"compressedSize,omitempty"`
	PartitionInfo    []PartitionInfo `json:"partitionInfo"`
}

type RowType struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	Length    int64  `json:"length"`
	Precision int64  `json:"precision"`
	Scale     int64  `json:"scale"`
	Nullable  bool   `json:"nullable"`
}

type execSQLUpdate struct {
	Type string // One of "status", "data", "metadata"

	Event execSQLEvent
}

type execSQLEvent struct {
	StatusMessage string             `json:"status_message,omitempty"` // Only for "status" type
	Data          [][]string         `json:"data,omitempty"`           // Only for "data" type
	Metadata      *sqlResultMetadata `json:"metadata,omitempty"`       // Only for "metadata" type

}

type sqlResultMetadata struct {
	TotalRows int       `json:"totalRows"`
	Columns   []RowType `json:"columns"`
}

var (
	defaultStatementTimeout = time.Minute
)

func kickoffRequest(ctx context.Context, apiURL, jwtToken string, statementReq StatementRequest) (string, error) {
	if statementReq.Timeout <= 0 {
		statementReq.Timeout = int(defaultStatementTimeout.Seconds())
	}

	// Set parameters for good date/time formatting.
	if statementReq.Parameters.DateOutputFormat == "" {
		statementReq.Parameters.DateOutputFormat = "YYYY-MM-DD"
	}
	if statementReq.Parameters.TimeOutputFormat == "" {
		statementReq.Parameters.TimeOutputFormat = "HH24:MI:SS"
	}
	if statementReq.Parameters.TimestampOutputFormat == "" {
		statementReq.Parameters.TimestampOutputFormat = "YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM"
	}
	if statementReq.Parameters.TimestampLTZOutputFormat == "" {
		statementReq.Parameters.TimestampLTZOutputFormat = "YYYY-MM-DD HH24:MI:SS.FF3"
	}
	if statementReq.Parameters.TimestampNTZOutputFormat == "" {
		statementReq.Parameters.TimestampNTZOutputFormat = "YYYY-MM-DD HH24:MI:SS.FF3"
	}
	if statementReq.Parameters.TimestampTZOutputFormat == "" {
		statementReq.Parameters.TimestampTZOutputFormat = "YYYY-MM-DD HH24:MI:SS.FF3"
	}

	// Encode the request payload to JSON.
	bodyBytes, err := json.Marshal(statementReq)
	if err != nil {
		return "", fmt.Errorf("error encoding JSON: %w", err)
	}

	// Create the POST request with the async query parameter.
	req, err := http.NewRequestWithContext(ctx, "POST", apiURL, bytes.NewBuffer(bodyBytes))
	if err != nil {
		return "", fmt.Errorf("error creating POST request: %w", err)
	}
	// Add the async parameter.
	q := req.URL.Query()
	q.Add("async", "true")
	req.URL.RawQuery = q.Encode()

	// Set the required headers.
	req.Header.Set("Authorization", "Bearer "+jwtToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "snowflake/cortex")
	req.Header.Set("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("error sending POST request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		bodyBytes, err := io.ReadAll(io.LimitReader(resp.Body, 16*1024))
		if err != nil {
			return "", fmt.Errorf("unexpected response status: %d (failed to read body: %w)", resp.StatusCode, err)
		}
		return "", fmt.Errorf("unexpected response status: %d (body %s)", resp.StatusCode, string(bodyBytes))
	}

	// Read the response body.
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("error reading POST response: %w", err)
	}

	// Since we do async, we know we'll get a query status.
	var queryStatus QueryStatus
	if err := json.Unmarshal(respBody, &queryStatus); err != nil {
		return "", fmt.Errorf("error parsing POST response JSON: %w", err)
	}

	if queryStatus.StatementHandle == "" {
		return "", fmt.Errorf("unexpected response: statement handle is empty")
	}
	return queryStatus.StatementHandle, nil
}

var (
	errSuccess = fmt.Errorf("success")
)

func execSnowflakeSQL(ctx context.Context, accountLocator, jwtToken string, statementReq StatementRequest, statusUpdateCh chan<- execSQLUpdate) error {
	// Build the API URL.
	apiURL := fmt.Sprintf("https://%s.snowflakecomputing.com/api/v2/statements", accountLocator)

	statementHandle, err := kickoffRequest(ctx, apiURL, jwtToken, statementReq)
	if err != nil {
		return err
	}

	select {
	case statusUpdateCh <- execSQLUpdate{Type: "status", Event: execSQLEvent{StatusMessage: fmt.Sprintf("Executing SQL (query ID: %s)", statementHandle)}}:
	case <-ctx.Done():
	}

	// Construct the URL for polling the statement status.
	statusURL := fmt.Sprintf("https://%s.snowflakecomputing.com/api/v2/statements/%s", accountLocator, statementHandle)

	// Poll until the statement execution completes.
	err = LoopEvery(ctx, time.Second, func() error {
		getReq, err := http.NewRequestWithContext(ctx, "GET", statusURL, nil)
		if err != nil {
			return fmt.Errorf("error creating GET request: %w", err)
		}
		getReq.Header.Set("Authorization", "Bearer "+jwtToken)
		getReq.Header.Set("Content-Type", "application/json")
		getReq.Header.Set("Accept", "application/json")
		getReq.Header.Set("User-Agent", "snowflake/cortex")
		getReq.Header.Set("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")

		getResp, err := http.DefaultClient.Do(getReq)
		if err != nil {
			return fmt.Errorf("error sending GET request: %w", err)
		}
		statusCode := getResp.StatusCode
		body, err := io.ReadAll(getResp.Body)
		getResp.Body.Close()
		if err != nil {
			// TODO: Maybe make this more resilient.
			return fmt.Errorf("status code: %d; error reading GET response: %w", statusCode, err)
		}
		switch statusCode {
		case http.StatusAccepted:
			// Query still running. Try again next time
			return nil
		case http.StatusOK:
		default:
			return fmt.Errorf("unexpected response status: %d (body %s)", statusCode, string(body))
		}
		// If we're here, the query execution has completed.

		var sqlResp ResultSet
		if err := json.Unmarshal(body, &sqlResp); err != nil {
			return fmt.Errorf("error parsing GET response JSON: %w", err)
		}

		// Send metadata
		select {
		case statusUpdateCh <- execSQLUpdate{Type: "sql.metadata", Event: execSQLEvent{Metadata: &sqlResultMetadata{TotalRows: len(sqlResp.Data), Columns: sqlResp.ResultSetMetaData.RowType}}}:
		case <-ctx.Done():
			return ctx.Err()
		}

		// Send data
		select {
		case statusUpdateCh <- execSQLUpdate{Type: "sql.data", Event: execSQLEvent{Data: sqlResp.Data}}:
		case <-ctx.Done():
			return ctx.Err()
		}

		// If there are additional partitions, we need to fetch them.
		// TODO: Implement this.
		return errSuccess
	})
	if errors.Is(err, errSuccess) {
		return nil
	}
	return err
}
