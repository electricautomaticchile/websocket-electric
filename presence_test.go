package main

import (
	"context"
	"testing"
	"time"
)

// TestPresenciaGlobal valida que el conteo global refleja los clientes
// reportados por las instancias vivas en Redis.
func TestPresenciaGlobal(t *testing.T) {
	cfg := testConfig()
	rdb, err := NewRedisClient(cfg)
	if err != nil {
		t.Skipf("Redis no disponible: %v", err)
	}
	defer rdb.Close()

	ctx := context.Background()
	// Limpiar claves de presencia previas.
	if keys, _ := rdb.Keys(ctx, presenceKeyPrefix+"*").Result(); len(keys) > 0 {
		rdb.Del(ctx, keys...)
	}

	// Simular dos instancias reportando presencia.
	p1 := NewPresence(rdb)
	p1.instanceID = "instancia-A"
	p2 := NewPresence(rdb)
	p2.instanceID = "instancia-B"

	p1.refresh(ctx, 40)
	p2.refresh(ctx, 60)
	time.Sleep(100 * time.Millisecond)

	total, err := p1.GlobalCount(ctx)
	if err != nil {
		t.Fatalf("GlobalCount falló: %v", err)
	}
	if total != 100 {
		t.Fatalf("conteo global esperado 100, obtenido %d", total)
	}
	t.Logf("✅ Presencia global correcta: 40 + 60 = %d clientes en 2 instancias", total)

	// Limpieza.
	rdb.Del(ctx, p1.key(), p2.key())
}
