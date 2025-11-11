package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/shopspring/decimal"
)

// Report represents a row in the ClickHouse table with user signal data.
type AirflowUserReport struct {
	UserID             int32     `ch:"user_id"`
	Name               string    `ch:"name"`
	Age                int32     `ch:"age"`
	Gender             string    `ch:"gender"`
	Email              string    `ch:"email"`
	Country            string    `ch:"country"`
	TotalSignals       int32     `ch:"total_signals"`
	AvgSignalDuration  float32   `ch:"avg_signal_duration_sec"`
	AvgSignalAmplitude float32   `ch:"avg_signal_amplitude"`
	MaxFrequency       int32     `ch:"max_frequency"`
	LastSignalTime     time.Time `ch:"last_signal_time"`
	MuscleUsageCount   string    `ch:"muscle_usage_count"`
}

// Report represents a row in the ClickHouse table with user signal data.
type UserReport struct {
	UserID             uint32          `ch:"user_id"`
	Name               string          `ch:"name"`
	Age                decimal.Decimal `ch:"age"`
	Gender             string          `ch:"gender"`
	Email              string          `ch:"email"`
	Country            string          `ch:"country"`
	TotalSignals       uint64          `ch:"total_signals"`
	AvgSignalDuration  float64         `ch:"avg_signal_duration_sec"`
	AvgSignalAmplitude float64         `ch:"avg_signal_amplitude"`
	MaxFrequency       uint32          `ch:"max_frequency"`
	LastSignalTime     time.Time       `ch:"last_signal_time"`
	MuscleUsageCount   uint64          `ch:"muscle_usage_count"`
}

func main() {
	// Initialize Echo
	e := echo.New()

	// Initialize ClickHouse connection
	chConn, err := connectToClickHouse()
	if err != nil {
		log.Fatalf("clickhouse connection error: %v", err)
	}
	defer chConn.Close()

	// Initialize MinIO client
	minioClient, err := minio.New("localhost:9000", &minio.Options{
		Creds:  credentials.NewStaticV4("minio_user", "minio_password", ""),
		Secure: false,
	})
	if err != nil {
		panic(err)
	}

	// Middleware
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())

	// Routes
	e.GET("/reports", func(c echo.Context) error {
		ctx := context.Background()
		email := c.Get("email").(string)

		bucketName := "reports"
		// Check if object exists
		_, err := minioClient.StatObject(ctx, bucketName, email, minio.StatObjectOptions{})
		if err == nil {
			// File exists
			// Return in JSON response
			return c.JSON(http.StatusOK, map[string]string{
				"cdn_url": fmt.Sprintf("http://localhost:8888/reports/%s", email),
			})
		}

		// File does not exist, get and upload it
		var response string
		if c.QueryParam("type") == "airflow" {
			var results []AirflowUserReport
			// Use Select to marshal query rows directly into the slice of structs
			if err := chConn.Select(ctx, &results, fmt.Sprintf("SELECT * FROM default.report_patient_activity_mart WHERE email = '%s'", email), 2); err != nil {
				return c.String(http.StatusInternalServerError, "Query error: "+err.Error())
			}
			for _, row := range results {
				response += fmt.Sprintf("TotalSignals=%d, LastSignalTime=%s, MaxFrequency=%s, Name=%s, Age=%s\n", row.TotalSignals, row.LastSignalTime, row.MaxFrequency, row.Name, row.Age)
			}

		} else {
			var results []UserReport
			// Use Select to marshal query rows directly into the slice of structs
			if err := chConn.Select(ctx, &results, fmt.Sprintf("SELECT * FROM default.emg_user_summary_mv WHERE email = '%s'", email), 2); err != nil {
				return c.String(http.StatusInternalServerError, "Query error: "+err.Error())
			}
			for _, row := range results {
				response += fmt.Sprintf("TotalSignals=%d, LastSignalTime=%s, MaxFrequency=%s, Name=%s, Age=%s\n", row.TotalSignals, row.LastSignalTime, row.MaxFrequency, row.Name, row.Age)
			}
		}

		// If error is not "object not found", return error
		minioErr, ok := err.(minio.ErrorResponse)
		if ok && minioErr.Code != "NoSuchKey" && minioErr.Code != "NotFound" {
			return c.String(http.StatusInternalServerError, "Failed to check file existence: "+err.Error())
		}

		reader := strings.NewReader(response)
		contentSize := int64(len(response))
		contentType := "text/plain"
		_, err = minioClient.PutObject(ctx, bucketName, email, reader, contentSize, minio.PutObjectOptions{
			ContentType: contentType,
		})
		if err != nil {
			return c.String(http.StatusInternalServerError, "Failed to upload string as file: "+err.Error())
		}

		// Return in JSON response
		return c.JSON(http.StatusOK, map[string]string{
			"cdn_url": fmt.Sprintf("http://localhost:8888/reports/%s", email),
		})
	}, UserIDFromJWT)

	// Start server
	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}
	e.Logger.Fatal(e.Start(":" + port))
}

// connectToClickHouse establishes connection to ClickHouse database
func connectToClickHouse() (clickhouse.Conn, error) {
	// Default connection parameters
	host := "localhost"
	port := 9431
	user := "default"
	password := ""
	database := "default"

	// Allow environment variables to override defaults
	if hostEnv := os.Getenv("CLICKHOUSE_HOST"); hostEnv != "" {
		host = hostEnv
	}
	if portEnv := os.Getenv("CLICKHOUSE_PORT"); portEnv != "" {
		port, _ = fmt.Sscanf(portEnv, "%d")
	}
	if userEnv := os.Getenv("CLICKHOUSE_USER"); userEnv != "" {
		user = userEnv
	}
	if passwordEnv := os.Getenv("CLICKHOUSE_PASSWORD"); passwordEnv != "" {
		password = passwordEnv
	}
	if dbEnv := os.Getenv("CLICKHOUSE_DATABASE"); dbEnv != "" {
		database = dbEnv
	}

	// Connect to ClickHouse
	// Initialize ClickHouse connection
	chConn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%v", host, port)},
		Auth: clickhouse.Auth{
			Database: database,
			Username: user,
			Password: password,
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("clickhouse connection error: %v", err)
		return nil, err
	}

	return chConn, nil
}

var jwtSecret = []byte("your-secret-key")

// Middleware to extract userID from JWT token in Authorization header
func UserIDFromJWT(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		host := "localhost:8080"
		if hostEnv := os.Getenv("REACT_APP_KEYCLOAK_URL"); hostEnv != "" {
			host = hostEnv
		}

		realm := "reports-realm"
		if realmEnv := os.Getenv("REACT_APP_KEYCLOAK_REALM"); realmEnv != "" {
			realm = realmEnv
		}

		jwksURL := fmt.Sprintf("http://%s/realms/%s/protocol/openid-connect/certs", host, realm)

		// Create the keyfunc.Keyfunc.
		jwks, err := keyfunc.NewDefault([]string{jwksURL})
		if err != nil {
			log.Fatalf("Failed to create JWK Set from resource at the given URL.\nError: %s", err)
		}

		authHeader := c.Request().Header.Get("Authorization")
		if authHeader == "" {
			return echo.NewHTTPError(http.StatusUnauthorized, "missing Authorization header")
		}

		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || parts[0] != "Bearer" {
			return echo.NewHTTPError(http.StatusUnauthorized, "invalid Authorization header format")
		}

		tokenString := parts[1]
		token, err := jwt.Parse(tokenString, jwks.Keyfunc)
		if err != nil || !token.Valid {
			return echo.NewHTTPError(http.StatusUnauthorized, "invalid or expired token")
		}

		claims, ok := token.Claims.(jwt.MapClaims)
		if !ok {
			return echo.NewHTTPError(http.StatusUnauthorized, "invalid token claims")
		}

		email, ok := claims["email"].(string)
		if !ok {
			return echo.NewHTTPError(http.StatusUnauthorized, "email claim missing or invalid")
		}

		// Save userID in context for handlers
		c.Set("email", email)

		return next(c)
	}
}
