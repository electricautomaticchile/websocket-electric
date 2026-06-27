package main

import (
	"log"
	"net/http"
	"strings"

	"github.com/gorilla/websocket"
)

// WSHandler gestiona el upgrade HTTP→WS y la autenticación por JWT.
type WSHandler struct {
	hub      *Hub
	cfg      *Config
	upgrader websocket.Upgrader
}

func NewWSHandler(hub *Hub, cfg *Config) *WSHandler {
	allowed := cfg.AllowedOrigins
	upgrader := websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		// Validación de origen para prevenir Cross-Site WebSocket Hijacking.
		CheckOrigin: func(r *http.Request) bool {
			origin := r.Header.Get("Origin")
			if origin == "" {
				// Apps nativas (sin Origin) — permitidas; el JWT sigue siendo obligatorio.
				return true
			}
			for _, a := range allowed {
				if a == origin {
					return true
				}
			}
			return false
		},
	}

	return &WSHandler{hub: hub, cfg: cfg, upgrader: upgrader}
}

// HandleConnect valida el token y, si es válido, hace el upgrade a WebSocket.
// El token se obtiene de la cookie auth_token (web) o del header Authorization
// Bearer (clientes nativos), igual que en la API.
func (h *WSHandler) HandleConnect(w http.ResponseWriter, r *http.Request) {
	tokenStr := extractToken(r)
	if tokenStr == "" {
		log.Printf("❌ WS: token no proporcionado")
		http.Error(w, "Token requerido", http.StatusUnauthorized)
		return
	}

	claims, err := ParseJWTClaims(tokenStr, h.cfg.JWTSecret)
	if err != nil {
		http.Error(w, "Token inválido", http.StatusUnauthorized)
		return
	}

	// Upgrade DESPUÉS de validar — no se debe escribir al ResponseWriter tras esto.
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	client := &Client{
		Hub:       h.hub,
		Conn:      conn,
		Send:      make(chan []byte, 256),
		UserID:    claims.UserID,
		UserType:  claims.UserType,
		EmpresaID: claims.EmpresaID,
	}

	h.hub.Register <- client
	go client.WritePump()
	go client.ReadPump()
}

// extractToken obtiene el JWT de la cookie auth_token o del header Bearer.
func extractToken(r *http.Request) string {
	if c, err := r.Cookie("auth_token"); err == nil && c.Value != "" {
		return c.Value
	}
	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
		return strings.TrimPrefix(h, "Bearer ")
	}
	// Fallback: token por query param (?token=...) para clientes que no pueden
	// fijar headers en el handshake WebSocket.
	if t := r.URL.Query().Get("token"); t != "" {
		return t
	}
	return ""
}
