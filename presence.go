package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// presenceKeyPrefix agrupa las claves de presencia por instancia del WS.
const presenceKeyPrefix = "ws:presence:"

// presenceTTL es el tiempo de vida de la clave de una instancia. Si una
// instancia muere, su entrada expira y deja de contarse.
const presenceTTL = 30 * time.Second

// presenceRefresh es cada cuánto cada instancia reporta su conteo local.
const presenceRefresh = 10 * time.Second

// Presence publica el número de clientes conectados de esta instancia en Redis,
// permitiendo un conteo global cuando hay varias instancias del WS Hub.
type Presence struct {
	rdb        *redis.Client
	instanceID string
}

func NewPresence(rdb *redis.Client) *Presence {
	host, _ := os.Hostname()
	if host == "" {
		host = "ws"
	}
	return &Presence{
		rdb:        rdb,
		instanceID: fmt.Sprintf("%s-%d", host, os.Getpid()),
	}
}

// Run refresca periódicamente la presencia de esta instancia hasta que el
// contexto se cancele. Al salir, borra su clave.
func (p *Presence) Run(ctx context.Context, hub *Hub) {
	ticker := time.NewTicker(presenceRefresh)
	defer ticker.Stop()

	p.refresh(ctx, hub.GetConnectedClients()) // primer reporte inmediato

	for {
		select {
		case <-ctx.Done():
			// Limpieza best-effort al apagar.
			delCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			p.rdb.Del(delCtx, p.key())
			cancel()
			return
		case <-ticker.C:
			p.refresh(ctx, hub.GetConnectedClients())
		}
	}
}

func (p *Presence) key() string {
	return presenceKeyPrefix + p.instanceID
}

func (p *Presence) refresh(ctx context.Context, localClients int) {
	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	p.rdb.Set(c, p.key(), localClients, presenceTTL)
}

// GlobalCount suma los clientes conectados de todas las instancias vivas.
func (p *Presence) GlobalCount(ctx context.Context) (int, error) {
	c, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	var total int
	var cursor uint64
	for {
		keys, next, err := p.rdb.Scan(c, cursor, presenceKeyPrefix+"*", 100).Result()
		if err != nil {
			return 0, err
		}
		for _, k := range keys {
			v, err := p.rdb.Get(c, k).Result()
			if err != nil {
				continue
			}
			if n, err := strconv.Atoi(v); err == nil {
				total += n
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return total, nil
}
