// Command websocket-electric es el servicio WebSocket Hub de Electric Automatic.
//
// Responsabilidades:
//   - Aceptar conexiones WebSocket autenticadas por JWT (misma clave que la API).
//   - Suscribirse a Redis Pub/Sub y reenviar eventos a los clientes conectados.
//   - Hacer broadcast en tiempo real por cliente, por empresa o global.
//
// La API REST no habla directamente con este servicio: publica eventos en Redis
// y el Hub los entrega. Esto permite escalar el WS horizontalmente sin sticky
// sessions, ya que cualquier instancia recibe todos los eventos vía Pub/Sub.
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	cfg := LoadConfig()

	redisClient, err := NewRedisClient(cfg)
	if err != nil {
		log.Fatalf("❌ Redis es requerido por el servicio WS: %v", err)
	}
	defer redisClient.Close()

	hub := NewHub()
	go hub.Run()

	subscriber := NewRedisSubscriber(redisClient, hub)
	subCtx, cancelSub := context.WithCancel(context.Background())
	go subscriber.Run(subCtx)

	wsHandler := NewWSHandler(hub, cfg)

	mux := http.NewServeMux()
	mux.HandleFunc("/ws/connect", wsHandler.HandleConnect)
	mux.HandleFunc("/ws", wsHandler.HandleConnect)

	// Health check para ALB / ECS.
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":    "ok",
			"service":   "websocket-electric",
			"clients":   hub.GetConnectedClients(),
			"timestamp": time.Now().UTC(),
		})
	})

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("✅ WebSocket Hub escuchando en puerto %s (%s)", cfg.Port, cfg.Environment)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("❌ %v", err)
		}
	}()

	// Graceful shutdown.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("🛑 Apagando WebSocket Hub...")

	cancelSub()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("⚠️ Error en shutdown: %v", err)
	}
	log.Println("✅ WebSocket Hub apagado")
}
