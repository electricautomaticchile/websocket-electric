package main

import (
	"log"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

// Config contiene la configuración del servicio WebSocket.
type Config struct {
	Port      string
	JWTSecret string

	// Redis
	RedisHost     string
	RedisPort     string
	RedisPassword string
	RedisDB       int
	RedisTLS      bool

	// CORS / orígenes permitidos para el handshake WebSocket
	AllowedOrigins []string

	Environment string
}

// LoadConfig carga la configuración desde variables de entorno.
// En desarrollo local usa un archivo .env si existe.
func LoadConfig() *Config {
	if err := godotenv.Load(); err != nil {
		log.Println("ℹ️ No se encontró archivo .env, usando variables de entorno del sistema")
	}

	cfg := &Config{
		Port:           getEnv("PORT", "8081"),
		JWTSecret:      getEnv("JWT_SECRET", ""),
		RedisHost:      getEnv("REDIS_HOST", "localhost"),
		RedisPort:      getEnv("REDIS_PORT", "6379"),
		RedisPassword:  getEnv("REDIS_PASSWORD", ""),
		RedisDB:        getEnvInt("REDIS_DB", 0),
		RedisTLS:       getEnvBool("REDIS_TLS", false),
		AllowedOrigins: splitAndTrim(getEnv("CORS_ORIGINS", "http://localhost:3000")),
		Environment:    getEnv("NODE_ENV", "development"),
	}

	applyRedisURL(cfg)

	if cfg.JWTSecret == "" {
		log.Fatal("❌ JWT_SECRET es requerido (debe ser la misma clave que usa la API).")
	}
	if len(cfg.JWTSecret) < 32 {
		log.Fatal("❌ JWT_SECRET debe tener al menos 32 caracteres.")
	}

	return cfg
}

// applyRedisURL permite configurar Redis con una única variable REDIS_URL
// (formato redis:// o rediss://), sobrescribiendo host/port/password/db.
func applyRedisURL(cfg *Config) {
	raw := strings.TrimSpace(os.Getenv("REDIS_URL"))
	if raw == "" {
		return
	}

	parsed, err := url.Parse(raw)
	if err != nil {
		log.Printf("⚠️ REDIS_URL inválida, usando REDIS_HOST/REDIS_PORT: %v", err)
		return
	}

	if host := parsed.Hostname(); host != "" {
		cfg.RedisHost = host
	}
	if port := parsed.Port(); port != "" {
		cfg.RedisPort = port
	}
	if password, ok := parsed.User.Password(); ok {
		cfg.RedisPassword = password
	}
	if db := strings.Trim(parsed.Path, "/"); db != "" {
		if n, err := strconv.Atoi(db); err == nil {
			cfg.RedisDB = n
		}
	}
	if parsed.Scheme == "rediss" {
		cfg.RedisTLS = true
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func getEnvBool(key string, def bool) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	switch v {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return def
	}
}

func splitAndTrim(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}
