package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

// Canales de Redis Pub/Sub usados como bus de eventos entre la API y el WS Hub.
const (
	// ChannelEvents recibe eventos dirigidos (cliente/empresa). La API publica
	// aquí un Event serializado en JSON.
	ChannelEvents = "ws:events"
	// ChannelBroadcast recibe eventos globales (scope "all").
	ChannelBroadcast = "ws:broadcast"
)

// NewRedisClient crea y verifica la conexión a Redis.
func NewRedisClient(cfg *Config) (*redis.Client, error) {
	opts := &redis.Options{
		Addr:     fmt.Sprintf("%s:%s", cfg.RedisHost, cfg.RedisPort),
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	}

	if cfg.RedisTLS {
		opts.TLSConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
			ServerName: cfg.RedisHost,
		}
	}

	client := redis.NewClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := client.Ping(ctx).Result(); err != nil {
		return nil, fmt.Errorf("no se pudo conectar a Redis (%s:%s): %w", cfg.RedisHost, cfg.RedisPort, err)
	}

	log.Printf("✅ Conectado a Redis en %s:%s", cfg.RedisHost, cfg.RedisPort)
	return client, nil
}

// RedisSubscriber se suscribe a los canales de eventos y reenvía cada evento
// al Hub para su broadcast.
type RedisSubscriber struct {
	client *redis.Client
	hub    *Hub
}

func NewRedisSubscriber(client *redis.Client, hub *Hub) *RedisSubscriber {
	return &RedisSubscriber{client: client, hub: hub}
}

// Run abre la suscripción y procesa mensajes hasta que el contexto se cancele.
// Se reconecta automáticamente ante errores transitorios.
func (s *RedisSubscriber) Run(ctx context.Context) {
	pubsub := s.client.Subscribe(ctx, ChannelEvents, ChannelBroadcast)
	defer pubsub.Close()

	ch := pubsub.Channel()
	log.Printf("📡 Suscrito a canales Redis: %s, %s", ChannelEvents, ChannelBroadcast)

	for {
		select {
		case <-ctx.Done():
			log.Println("🛑 Subscriber detenido")
			return
		case msg, ok := <-ch:
			if !ok {
				log.Println("⚠️ Canal Redis cerrado, reintentando suscripción...")
				time.Sleep(time.Second)
				pubsub = s.client.Subscribe(ctx, ChannelEvents, ChannelBroadcast)
				ch = pubsub.Channel()
				continue
			}
			s.dispatch(msg.Payload)
		}
	}
}

// dispatch deserializa el Event recibido y lo enruta al Hub.
func (s *RedisSubscriber) dispatch(payload string) {
	var event Event
	if err := json.Unmarshal([]byte(payload), &event); err != nil {
		log.Printf("⚠️ Evento Redis inválido: %v", err)
		return
	}

	switch event.Scope {
	case ScopeCliente:
		s.hub.BroadcastToCliente(event.TargetID, event.Message)
	case ScopeEmpresa:
		s.hub.BroadcastToEmpresa(event.TargetID, event.Message)
	case ScopeAll:
		s.hub.BroadcastToAll(event.Message)
	default:
		log.Printf("⚠️ Scope de evento desconocido: %q", event.Scope)
	}
}
