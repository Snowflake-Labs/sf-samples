package cortexanalystapp

import (
	"crypto/rsa"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	authKeyEnvVar           = "SNOWFLAKE_ACCOUNT_PRIVATE_KEY_FILE"
	accountIdentifierEnvVar = "SNOWFLAKE_ACCOUNT_LOCATOR"
	userNameEnvVar          = "SNOWFLAKE_USER"
	publicKeyFPEnvVar       = "SNOWFLAKE_PUBLIC_KEY_FP"
)

func readPrivateKey() (*rsa.PrivateKey, error) {
	file := os.Getenv(authKeyEnvVar)
	if file == "" {
		return nil, fmt.Errorf("env var %v not found", authKeyEnvVar)
	}

	key, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("error reading %v: %w", file, err)
	}
	return ReadAvalancheRSAKey(key)
}

func generateJWT(privKey *rsa.PrivateKey) (string, error) {
	account := os.Getenv(accountIdentifierEnvVar)
	user := os.Getenv(userNameEnvVar)
	publicKeyFP := os.Getenv(publicKeyFPEnvVar)
	if account == "" || user == "" || publicKeyFP == "" {
		return "", fmt.Errorf("missing required environment variables (got %s=%s, %s=%s, %s=%s)",
			accountIdentifierEnvVar, account, userNameEnvVar, user, publicKeyFPEnvVar, publicKeyFP)
	}

	// Process the account identifier.
	// If the account identifier does not contain ".global", strip off the region/cloud parts.
	if !strings.Contains(account, ".global") {
		if idx := strings.Index(account, "."); idx > 0 {
			account = account[:idx]
		} else if idx := strings.Index(account, "-"); idx > 0 {
			// Handle the replication case.
			account = account[:idx]
		}
	}

	// Convert account and user to uppercase and build the fully qualified username.
	account = strings.ToUpper(account)
	user = strings.ToUpper(user)
	qualifiedUsername := account + "." + user

	// Load your RSA private key.
	// Make sure "private_key.pem" contains your RSA private key.

	// Get the current time in UTC.
	now := time.Now().UTC()
	// Specify the lifetime for the JWT (at most 1 hour; here 59 minutes).
	lifetime := 59 * time.Minute

	// Create the claims (payload) for the JWT.
	claims := jwt.RegisteredClaims{
		// Issuer is the fully qualified username concatenated with the public key fingerprint.
		Issuer: qualifiedUsername + "." + publicKeyFP,
		// Subject is the fully qualified username.
		Subject: qualifiedUsername,
		// IssuedAt and ExpiresAt are set using the current time and the specified lifetime.
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(now.Add(lifetime)),
	}

	// Create a new token with the RS256 signing method and the claims.
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)

	// Sign the token using the RSA private key.
	tokenString, err := token.SignedString(privKey)
	if err != nil {
		return "", fmt.Errorf("error signing token: %w", err)
	}
	return tokenString, nil
}
