package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	sf "github.com/snowflakedb/gosnowflake"
)

const (
	EnvSnowflakeAccount  = "SNOWFLAKE_ACCOUNT"
	EnvSnowflakeHost     = "SNOWFLAKE_HOST"
	EnvSnowflakeProtocol = "SNOWFLAKE_PROTOCOL"
	EnvSnowflakePort     = "SNOWFLAKE_PORT"
	EnvSnowflakeDatabase = "SNOWFLAKE_DATABASE"
	EnvSnowflakeSchema   = "SNOWFLAKE_SCHEMA"
	EnvWarehouse         = "WAREHOUSE"
	EnvUser              = "SNOWFLAKE_USER"
	EnvPassword          = "SNOWFLAKE_PASSWORD"
	EnvRole              = "ROLE"
	TokenPath            = "/snowflake/session/token"
)

func main() {
	log.Printf("START")

	conn, err := openConnection()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Connection succeeded!")

	query := "SELECT 'successfully connected to '||current_account();"
	rows, _, err := runQuery(conn, query)
	if err != nil {
		log.Fatal(err)
	}
	if len(rows) == 0 {
		log.Fatal(fmt.Errorf("%s returns empty result", query))
	}
	log.Printf("%s returns %s", query, rows[0][0])
}

func openConnection() (*sql.DB, error) {
	// get Snowflake connection config
	cfg, err := getSfConfig()
	if err != nil {
		return nil, fmt.Errorf("error getting snowflake config. err: %v", err)
	}

	// Create DSN (DataSourceName) from config
	dsn, err := sf.DSN(&cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create DSN from Config. err: %v", err)
	}

	// Establish connection with snowflake
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect. err: %v", err)
	}

	return db, nil
}

func getSfConfig() (sf.Config, error) {
	var cfg sf.Config

	if _, err := os.Stat(TokenPath); err == nil {
		// token
		token, err := getTokenFromFile(TokenPath)
		if err != nil {
			return cfg, fmt.Errorf("error getting credentials from token file %s. err: %v", TokenPath, err)
		}
		cfg = sf.Config{
			Account:       os.Getenv(EnvSnowflakeAccount),
			Host:          os.Getenv(EnvSnowflakeHost),
			Token:         token,
			Authenticator: sf.AuthTypeOAuth,
			Database:      os.Getenv(EnvSnowflakeDatabase),
			Schema:        os.Getenv(EnvSnowflakeSchema),
			Warehouse:     os.Getenv(EnvWarehouse),
		}
	} else {
		// basic auth
		cfg = sf.Config{
			Account:   os.Getenv(EnvSnowflakeAccount),
			Host:      os.Getenv(EnvSnowflakeHost),
			User:      os.Getenv(EnvUser),
			Password:  os.Getenv(EnvPassword),
			Role:      os.Getenv(EnvRole),
			Database:  os.Getenv(EnvSnowflakeDatabase),
			Schema:    os.Getenv(EnvSnowflakeSchema),
			Warehouse: os.Getenv(EnvWarehouse),
		}
	}

	return cfg, nil
}

func getTokenFromFile(filePath string) (string, error) {
	// Open the file.
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	// Read the file contents into a string.
	fileContents, err := ioutil.ReadAll(file)
	if err != nil {
		return "", err
	}
	// Close the file.
	defer file.Close()

	return string(fileContents), nil
}

// Returns:
//   - rows: A 2D string slice representing the result rows of the executed query.
//   - columns: A string slice containing the names of the columns in the result set.
//   - error: An error, if any, encountered during the database connection, query execution, or data retrieval.
func runQuery(db *sql.DB, query string) ([][]string, []string, error) {

	// Execute query
	rows, err := db.Query(query)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to run query. %v, err: %v", query, err)
	}
	defer rows.Close()

	// Get column names and types
	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, fmt.Errorf("error getting column names. err: %v", err)
	}

	// Create a slice to hold the values
	values := make([]interface{}, len(columns))
	for i := range values {
		var v interface{}
		values[i] = &v
	}

	// Create a slice to store the rows
	var result [][]string

	// Iterate through the rows
	for rows.Next() {
		// Scan the current row into the values slice
		if err := rows.Scan(values...); err != nil {
			return nil, nil, fmt.Errorf("error scanning row. err: %v", err)
		}

		// Convert each value to a string and append it to the result slice
		rowValues := make([]string, len(columns))
		for i := range columns {
			if values[i] != nil {
				// Convert the underlying value to a string
				strVal := fmt.Sprintf("%v", *values[i].(*interface{}))
				rowValues[i] = strVal
			}
		}
		result = append(result, rowValues)
	}

	// Check for errors during iteration
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("error iterating over rows. err: %v", err)
	}

	return result, columns, nil
}
