package main

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	oidc "github.com/coreos/go-oidc"
	"github.com/google/uuid"
	"github.com/gorilla/sessions"
	"github.com/patrickmn/go-cache"
	"golang.org/x/oauth2"
)

// AuditLogger provides structured logging for security events
type AuditLogger struct {
	logger *log.Logger
}

// NewAuditLogger creates a new audit logger
func NewAuditLogger() *AuditLogger {
	return &AuditLogger{
		logger: log.New(os.Stdout, "AUDIT: ", log.LstdFlags|log.Lshortfile),
	}
}

// LogTokenOperation logs token-related operations
func (al *AuditLogger) LogTokenOperation(operation, userID, sessionID string, details map[string]interface{}) {
	logEntry := map[string]interface{}{
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"operation": operation,
		"userID":    userID,
		"sessionID": sessionID,
	}

	for k, v := range details {
		logEntry[k] = v
	}

	al.logger.Printf("%+v", logEntry)
}

// Encryption key - in production, this should be loaded from a secure secret management system
var encryptionKey []byte
var oidcProvider oidc.Provider
var oidcConfig oidc.Config
var oauth2Config oauth2.Config
var idTokenVerifier oidc.IDTokenVerifier
var store *sessions.CookieStore
var appCache *cache.Cache
var cacheMutex = &sync.RWMutex{}
var auditLogger = NewAuditLogger()

// UserTokens represents the structure for storing user authentication tokens
type UserTokens struct {
	AccessToken  string    `json:"access_token"`
	RefreshToken string    `json:"refresh_token"`
	ExpiresAt    time.Time `json:"expires_at"`
	TokenType    string    `json:"token_type"`
	UserID       string    `json:"user_id"`
	SessionID    string    `json:"session_id"`
}

// EncryptedUserTokens wraps UserTokens for encryption purposes
type EncryptedUserTokens struct {
	EncryptedData []byte `json:"encrypted_data"`
}

// Encrypt encrypts the data using AES-256-GCM
func encrypt(data []byte) ([]byte, error) {
	block, err := aes.NewCipher(encryptionKey)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	ciphertext := gcm.Seal(nonce, nonce, data, nil)
	return ciphertext, nil
}

// Decrypt decrypts the data using AES-256-GCM
func decrypt(ciphertext []byte) ([]byte, error) {
	block, err := aes.NewCipher(encryptionKey)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, errors.New("ciphertext too short")
	}

	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}

	return plaintext, nil
}

func init() {
	// Initialize encryption key from environment variable or generate a new one
	key := os.Getenv("ENCRYPTION_KEY")
	if key == "" {
		// Generate a new 32-byte key for AES-256
		encryptionKey = make([]byte, 32)
		if _, err := rand.Read(encryptionKey); err != nil {
			log.Fatal("Failed to generate encryption key: ", err)
		}
		// In production, you'd want to store this key securely and load it from a secret store
		log.Println("Generated new encryption key. In production, load from secure secret store.")
	} else {
		// Convert hex-encoded key to bytes
		var err error
		encryptionKey, err = hex.DecodeString(key)
		if err != nil {
			log.Fatal("Invalid encryption key format: ", err)
		}
	}

	// Initialize session store with proper key
	sessionKey := os.Getenv("SESSION_KEY")
	if sessionKey == "" {
		// Generate a default key for development (should be changed in production)
		sessionKeyBytes := make([]byte, 32)
		if _, err := rand.Read(sessionKeyBytes); err != nil {
			log.Fatal("Failed to generate default session key: ", err)
		}
		sessionKey = string(sessionKeyBytes)
		os.Setenv("SESSION_KEY", sessionKey)
		log.Println("Generated default session key for development")
	}
	store = sessions.NewCookieStore([]byte(sessionKey))

	oidcProvider = *createOidcProvider(context.Background())
	oidcConfig, oauth2Config = createConfig(oidcProvider)
	idTokenVerifier = *oidcProvider.Verifier(&oidcConfig)
	appCache = cache.New(30*time.Minute, 5*time.Minute) // 30 min TTL, 5 min cleanup
}

func createOidcProvider(ctx context.Context) *oidc.Provider {
	provider, err := oidc.NewProvider(ctx, "http://localhost:8080/realms/reports-realm")

	if err != nil {
		log.Fatal("Failed to fetch discovery document: ", err)
	}

	return provider
}

func createConfig(provider oidc.Provider) (oidc.Config, oauth2.Config) {
	oidcConfig := &oidc.Config{
		ClientID: "pkce",
	}

	config := oauth2.Config{
		ClientID:     oidcConfig.ClientID,
		ClientSecret: "DHPuGp1ESeufECeNCXZvadk6IP1AI2ww",
		Endpoint:     provider.Endpoint(),
		RedirectURL:  "http://localhost:8081/auth/callback",
		Scopes:       []string{oidc.ScopeOpenID, "profile", "email"},
	}

	return *oidcConfig, config
}

func main() {
	// Create a new HTTP server
	server := &http.Server{
		Addr:    "localhost:8081",
		Handler: nil, // We'll register our handlers directly with the default mux
	}

	// Register our handlers with the default mux
	http.HandleFunc("/", redirectHandler)
	http.HandleFunc("/auth/callback", callbackHandler)
	http.HandleFunc("/reports", reportsHandler2)

	log.Printf("To authenticate go to http://%s/", "localhost:8081")

	// Create channel to listen for interrupt signals
	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	// Start the server in a goroutine
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	// Wait for interrupt signal
	<-done
	log.Println("Shutting down server...")

	// Create a context with timeout for graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Attempt graceful shutdown
	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Server exited properly")
}

func reportsHandler2(resp http.ResponseWriter, req *http.Request) {
	// Middleware to get access token from appCache and use for the request
	session, _ := store.Get(req, "auth")

	// Check if user is authenticated by checking session values
	if session.Values["expires"] == nil {
		// User not authenticated, redirect to login
		http.Redirect(resp, req, "/", http.StatusFound)
		return
	}

	// Get the subject (user ID) from session
	subject, ok := session.Values["subject"].(string)
	if !ok {
		// Subject not found, redirect to login
		http.Redirect(resp, req, "/", http.StatusFound)
		return
	}

	// Try to get access token from appCache using session-bound key
	// Using session name as prefix for cache key
	cacheKey := fmt.Sprintf("%s:%s", session.Values["session_id"], subject)

	// Get a valid token (refreshing if needed)
	validToken, err := getValidToken(cacheKey)
	if err != nil {
		// Failed to get a valid token, redirect to login
		log.Printf("Failed to get valid token: %v", err)
		redirectHandler(resp, req)
		return
	}

	// Log token access
	auditLogger.LogTokenOperation(
		"TOKEN_ACCESS",
		subject,
		session.Values["session_id"].(string),
		map[string]interface{}{
			"token_type": validToken.TokenType,
			"expires_at": validToken.ExpiresAt.Format(time.RFC3339),
		},
	)

	// Create new HTTP request
	reportReq, err := http.NewRequest("GET", "http://localhost:8082/reports", nil)
	if err != nil {
		http.Error(resp, "Failed to create request", http.StatusInternalServerError)
		return
	}

	// Add Authorization Bearer header
	bearer := "Bearer " + validToken.AccessToken
	reportReq.Header.Set("Authorization", bearer)

	// Create HTTP client and execute request
	client := &http.Client{}
	reportResp, err := client.Do(reportReq)
	if err != nil {
		http.Error(resp, "Failed to call other service", http.StatusInternalServerError)
		return
	}
	defer reportResp.Body.Close()

	// Read the response body from the other service
	body, err := io.ReadAll(reportResp.Body)
	if err != nil {
		http.Error(resp, "Failed to read response from the other service", http.StatusInternalServerError)
		return
	}
	// Set CORS headers
	resp.Header().Set("Access-Control-Allow-Origin", "http://localhost:3000")
	resp.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	resp.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
	resp.Header().Set("Access-Control-Allow-Credentials", "true")
	resp.Header().Set("Access-Control-Max-Age", "86400")
	resp.Header().Set("Content-Type", "application/json")
	resp.Write(body)
}

func reportsHandler(resp http.ResponseWriter, req *http.Request) {
	// Middleware to get access token from appCache and use for the request
	session, _ := store.Get(req, "auth")

	// Check if user is authenticated by checking session values
	if session.Values["expires"] == nil {
		// User not authenticated, redirect to login
		http.Redirect(resp, req, "/", http.StatusFound)
		return
	}

	// Get the subject (user ID) from session
	subject, ok := session.Values["subject"].(string)
	if !ok {
		// Subject not found, redirect to login
		http.Redirect(resp, req, "/", http.StatusFound)
		return
	}

	// Try to get access token from appCache using session-bound key
	// Using session name as prefix for cache key
	cacheKey := fmt.Sprintf("%s:%s", session.Values["session_id"], subject)

	// Get a valid token (refreshing if needed)
	validToken, err := getValidToken(cacheKey)
	if err != nil {
		// Failed to get a valid token, redirect to login
		log.Printf("Failed to get valid token: %v", err)
		redirectHandler(resp, req)
		return
	}

	// Log token access
	auditLogger.LogTokenOperation(
		"TOKEN_ACCESS",
		subject,
		session.Values["session_id"].(string),
		map[string]interface{}{
			"token_type": validToken.TokenType,
			"expires_at": validToken.ExpiresAt.Format(time.RFC3339),
		},
	)

	// Set CORS headers
	resp.Header().Set("Access-Control-Allow-Origin", "http://localhost:3000")
	resp.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	resp.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
	resp.Header().Set("Access-Control-Allow-Credentials", "true")
	resp.Header().Set("Access-Control-Max-Age", "86400")
	resp.Header().Set("Content-Type", "application/json")

	// Prepare the response data
	responseData := map[string]interface{}{
		"access_token":  validToken.AccessToken,
		"refresh_token": validToken.RefreshToken,
		"expires_at":    validToken.ExpiresAt.Format(time.RFC3339),
		"token_type":    validToken.TokenType,
		"user_id":       validToken.UserID,
		"session_id":    validToken.SessionID,
	}

	// Marshal the response data to JSON
	jsonData, err := json.Marshal(responseData)
	if err != nil {
		log.Printf("Failed to marshal JSON response: %v", err)
		http.Error(resp, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Write the JSON response
	resp.WriteHeader(http.StatusOK)
	resp.Write(jsonData)
}

func redirectHandler(resp http.ResponseWriter, r *http.Request) {
	pkceCode, err := Generate()
	addPkceToSession(pkceCode, r, resp)
	state := addStateCookie(resp)

	if err != nil {
		http.Error(resp, "Failed to generate pkce challenge", http.StatusInternalServerError)
		return
	}

	http.Redirect(resp, r, oauth2Config.AuthCodeURL(state, pkceCode.Challenge(), pkceCode.Method()), http.StatusFound)
}

func callbackHandler(resp http.ResponseWriter, req *http.Request) {
	err := checkStateAndExpireCookie(req, resp)

	if err != nil {
		redirectHandler(resp, req)
		return
	}

	tokenResponse, err := exchangeCode(req)

	if err != nil {
		http.Error(resp, "Failed to exchange code", http.StatusBadRequest)
		return
	}

	idToken, err := validateIDToken(tokenResponse, req)

	if err != nil {
		http.Error(resp, "Failed to validate id_token", http.StatusUnauthorized)
		return
	}

	handleSuccessfulAuthentication(tokenResponse, *idToken, resp, req)
}

func addStateCookie(resp http.ResponseWriter) string {
	expire := time.Now().Add(1 * time.Minute)
	value := uuid.New().String()

	cookie := http.Cookie{
		Name:     "p_state",
		Value:    value,
		Expires:  expire,
		HttpOnly: true,
		Secure:   true,
	}

	http.SetCookie(resp, &cookie)

	return value
}

func addPkceToSession(code Code, req *http.Request, resp http.ResponseWriter) {
	// Get session
	session, _ := store.Get(req, "auth")

	// Store PKCE code in session
	session.Values["pkce"] = string(code)

	// Save session
	session.Save(req, resp)
}

func expireCookie(name string, resp http.ResponseWriter) {
	cookie := &http.Cookie{
		Name:     "p_state",
		Value:    "",
		MaxAge:   -1,
		HttpOnly: true,
	}

	http.SetCookie(resp, cookie)
}

func checkStateAndExpireCookie(req *http.Request, resp http.ResponseWriter) error {
	state, err := req.Cookie("p_state")

	expireCookie("p_state", resp)

	if err != nil {
		return errors.New("state cookie not set")
	}

	if req.URL.Query().Get("state") != state.Value {
		return errors.New("invalid state")
	}

	return nil
}

func exchangeCode(req *http.Request) (*oauth2.Token, error) {
	httpClient := &http.Client{Timeout: 2 * time.Second}
	ctx := context.WithValue(req.Context(), oauth2.HTTPClient, httpClient)

	// Get session to retrieve PKCE code
	session, err := store.Get(req, "auth")
	if err != nil {
		return nil, err
	}

	pkceValue, ok := session.Values["pkce"].(string)
	if !ok {
		return nil, errors.New("pkce value not found in session")
	}

	pkceCode := Code(pkceValue)

	tokenResponse, err := oauth2Config.Exchange(ctx, req.URL.Query().Get("code"), pkceCode.Verifier())

	if err != nil {
		return nil, err
	}

	return tokenResponse, nil
}

func validateIDToken(tokenResponse *oauth2.Token, req *http.Request) (*oidc.IDToken, error) {
	rawIDToken, ok := tokenResponse.Extra("id_token").(string)

	if !ok {
		return nil, errors.New("id_token is not in the token response")
	}

	idToken, err := idTokenVerifier.Verify(req.Context(), rawIDToken)

	if err != nil {
		return nil, err
	}

	return idToken, nil
}

func handleSuccessfulAuthentication(tokenResponse *oauth2.Token, idToken oidc.IDToken, resp http.ResponseWriter, req *http.Request) {
	payload := struct {
		TokenResponse *oauth2.Token
		IDToken       *json.RawMessage
	}{tokenResponse, new(json.RawMessage)}

	if err := idToken.Claims(&payload.IDToken); err != nil {
		return
	}

	// Get session first
	session, _ := store.Get(req, "auth")

	// Create UserTokens object with both access and refresh tokens
	sessionID := uuid.New().String()
	userTokens := UserTokens{
		AccessToken:  tokenResponse.AccessToken,
		RefreshToken: tokenResponse.RefreshToken,
		ExpiresAt:    tokenResponse.Expiry,
		TokenType:    tokenResponse.TokenType,
		UserID:       idToken.Subject,
		SessionID:    sessionID, // Using UUID as session identifier
	}

	// Use session-bound cache key
	cacheKey := fmt.Sprintf("%s:%s", userTokens.SessionID, idToken.Subject)

	// Log token creation
	auditLogger.LogTokenOperation(
		"TOKEN_CREATION",
		idToken.Subject,
		userTokens.SessionID,
		map[string]interface{}{
			"token_type": userTokens.TokenType,
			"expires_at": userTokens.ExpiresAt.Format(time.RFC3339),
		},
	)

	// Encrypt the user tokens before storing in cache
	userTokensJSON, err := json.Marshal(userTokens)
	if err != nil {
		log.Printf("Failed to marshal user tokens: %v", err)
		http.Error(resp, "Internal server error", http.StatusInternalServerError)
		return
	}

	encryptedData, err := encrypt(userTokensJSON)
	if err != nil {
		log.Printf("Failed to encrypt user tokens: %v", err)
		http.Error(resp, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Wrap encrypted data in EncryptedUserTokens struct
	encryptedTokens := EncryptedUserTokens{
		EncryptedData: encryptedData,
	}

	// Acquire write lock for cache modification
	cacheMutex.Lock()
	appCache.Set(cacheKey, encryptedTokens, cache.NoExpiration)
	cacheMutex.Unlock()

	// Set some session values.
	session.Values["expires"] = tokenResponse.ExpiresIn
	session.Values["subject"] = idToken.Subject
	session.Values["session_id"] = sessionID
	session.Options = &sessions.Options{
		Path:     "/",
		MaxAge:   86400 * 7,
		HttpOnly: true,
		Secure:   true,
	}
	// Save it before we write to the response/return from the handler.
	err = session.Save(req, resp)
	if err != nil {
		http.Error(resp, err.Error(), http.StatusInternalServerError)
		return
	}

	http.Redirect(resp, req, "http://localhost:3000/", http.StatusFound)
}

// Generate generates a new random PKCE code.
func Generate() (Code, error) { return generate(rand.Reader) }

func generate(rand io.Reader) (Code, error) {
	// From https://tools.ietf.org/html/rfc7636#section-4.1:
	//   code_verifier = high-entropy cryptographic random STRING using the
	//   unreserved characters [A-Z] / [a-z] / [0-9] / "-" / "." / "_" / "~"
	//   from Section 2.3 of [RFC3986], with a minimum length of 43 characters
	//   and a maximum length of 128 characters.
	var buf [32]byte
	if _, err := io.ReadFull(rand, buf[:]); err != nil {
		return "", fmt.Errorf("could not generate PKCE code: %w", err)
	}
	return Code(hex.EncodeToString(buf[:])), nil
}

// Code implements the basic options required for RFC 7636: Proof Key for Code Exchange (PKCE).
type Code string

// Challenge returns the OAuth2 auth code parameter for sending the PKCE code challenge.
func (p *Code) Challenge() oauth2.AuthCodeOption {
	b := sha256.Sum256([]byte(*p))
	return oauth2.SetAuthURLParam("code_challenge", base64.RawURLEncoding.EncodeToString(b[:]))
}

// Method returns the OAuth2 auth code parameter for sending the PKCE code challenge method.
func (p *Code) Method() oauth2.AuthCodeOption {
	return oauth2.SetAuthURLParam("code_challenge_method", "S256")
}

// Verifier returns the OAuth2 auth code parameter for sending the PKCE code verifier.
func (p *Code) Verifier() oauth2.AuthCodeOption {
	return oauth2.SetAuthURLParam("code_verifier", string(*p))
}

// isTokenExpired checks if a token is expired or will expire soon
func isTokenExpired(token UserTokens, buffer time.Duration) bool {
	return time.Now().Add(buffer).After(token.ExpiresAt)
}

// refreshToken attempts to refresh an expired access token using the refresh token
func refreshToken(refreshToken string) (*oauth2.Token, error) {
	// Create a new context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Use the same oauth2 config to exchange refresh token for new access token
	token, err := oauth2Config.TokenSource(ctx, &oauth2.Token{
		RefreshToken: refreshToken,
	}).Token()

	if err != nil {
		return nil, fmt.Errorf("failed to refresh token: %w", err)
	}

	return token, nil
}

// getValidToken retrieves a valid token, refreshing it if necessary
func getValidToken(cacheKey string) (*UserTokens, error) {
	// Acquire read lock for cache access
	cacheMutex.RLock()
	token, found := appCache.Get(cacheKey)
	cacheMutex.RUnlock()

	if !found {
		return nil, errors.New("token not found in cache")
	}

	// Cast token to EncryptedUserTokens
	encryptedTokens, ok := token.(EncryptedUserTokens)
	if !ok {
		return nil, errors.New("invalid token type")
	}

	// Log token decryption
	auditLogger.LogTokenOperation(
		"TOKEN_DECRYPT",
		"", // userID will be extracted from cacheKey in the function
		"", // sessionID will be extracted from cacheKey in the function
		map[string]interface{}{
			"cache_key": cacheKey,
		},
	)

	// Decrypt the cached data
	decryptedData, err := decrypt(encryptedTokens.EncryptedData)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt token: %w", err)
	}

	// Unmarshal the decrypted data into UserTokens
	var userTokens UserTokens
	if err := json.Unmarshal(decryptedData, &userTokens); err != nil {
		return nil, fmt.Errorf("failed to unmarshal token: %w", err)
	}

	// Check if token is still valid (with 5 minute buffer for safety)
	if !isTokenExpired(userTokens, 20*time.Second) {
		return &userTokens, nil
	}

	// Token is expired, try to refresh it
	if userTokens.RefreshToken == "" {
		return nil, errors.New("no refresh token available")
	}

	// Log token refresh attempt
	auditLogger.LogTokenOperation(
		"TOKEN_REFRESH_ATTEMPT",
		userTokens.UserID,
		userTokens.SessionID,
		map[string]interface{}{
			"expires_at": userTokens.ExpiresAt.Format(time.RFC3339),
		},
	)

	// Attempt to refresh the token
	newToken, err := refreshToken(userTokens.RefreshToken)
	if err != nil {
		// Log refresh failure
		auditLogger.LogTokenOperation(
			"TOKEN_REFRESH_FAILURE",
			userTokens.UserID,
			userTokens.SessionID,
			map[string]interface{}{
				"error": err.Error(),
			},
		)

		return nil, fmt.Errorf("failed to refresh token: %w", err)
	}

	// Log successful refresh
	auditLogger.LogTokenOperation(
		"TOKEN_REFRESH_SUCCESS",
		userTokens.UserID,
		userTokens.SessionID,
		map[string]interface{}{
			"old_expires_at": userTokens.ExpiresAt.Format(time.RFC3339),
			"new_expires_at": newToken.Expiry.Format(time.RFC3339),
		},
	)

	// Create new UserTokens with refreshed access token
	refreshedTokens := UserTokens{
		AccessToken:  newToken.AccessToken,
		RefreshToken: userTokens.RefreshToken, // Keep the same refresh token
		ExpiresAt:    newToken.Expiry,
		TokenType:    newToken.TokenType,
		UserID:       userTokens.UserID,
		SessionID:    userTokens.SessionID,
	}

	// Encrypt the refreshed tokens
	refreshedTokensJSON, err := json.Marshal(refreshedTokens)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal refreshed tokens: %w", err)
	}

	encryptedData, err := encrypt(refreshedTokensJSON)
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt refreshed tokens: %w", err)
	}

	// Wrap encrypted data in EncryptedUserTokens struct
	encryptedRefreshedTokens := EncryptedUserTokens{
		EncryptedData: encryptedData,
	}

	// Update cache with refreshed token
	cacheMutex.Lock()
	appCache.Set(cacheKey, encryptedRefreshedTokens, cache.NoExpiration)
	cacheMutex.Unlock()

	return &refreshedTokens, nil
}
