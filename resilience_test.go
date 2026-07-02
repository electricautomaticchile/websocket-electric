package main

import (
	"context"
	"encoding/json"
	"os/exec"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestResilienciaRedisReinicio valida que, si Redis se cae y vuelve, el
// subscriber del WS reconecta automáticamente y la entrega de eventos se
// reanuda sin reiniciar el servicio. Equivalente a un test de caos.
//
// Requiere el contenedor Docker "test-redis". Se omite si docker no está.
func TestResilienciaRedisReinicio(t *testing.T) {
	if testing.Short() {
		t.Skip("omitido en -short")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker no disponible")
	}

	wsURL, pub, cleanup := setupServer(t)
	defer cleanup()

	token := makeToken(t, "cliente-res", "cliente", "")
	c := dial(t, wsURL, token)
	defer c.Close()
	time.Sleep(150 * time.Millisecond)

	// 1. Verificar entrega ANTES de la caída.
	publishEvent(t, pub, ChannelEvents, Event{
		Scope: ScopeCliente, TargetID: "cliente-res",
		Message: Message{Type: MessageTypeNotification, Timestamp: time.Now(), Data: map[string]interface{}{"fase": "antes"}},
	})
	if _, err := readMessage(c, 3*time.Second); err != nil {
		t.Fatalf("no hubo entrega antes de la caída: %v", err)
	}
	t.Logf("✅ Entrega OK antes de la caída de Redis")

	// 2. Reiniciar Redis (simula caída + recuperación).
	t.Logf("🔻 Reiniciando el contenedor Redis...")
	if out, err := exec.Command("docker", "restart", "test-redis").CombinedOutput(); err != nil {
		t.Skipf("no se pudo reiniciar Redis (%v): %s", err, out)
	}

	// El publisher del test también perdió su conexión; reconectar.
	time.Sleep(3 * time.Second)
	pub2 := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer pub2.Close()

	// Esperar a que el subscriber del WS reconecte (reintenta cada ~1s).
	var recovered bool
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		data, _ := json.Marshal(Event{
			Scope: ScopeCliente, TargetID: "cliente-res",
			Message: Message{Type: MessageTypeNotification, Timestamp: time.Now(), Data: map[string]interface{}{"fase": "despues"}},
		})
		if err := pub2.Publish(context.Background(), ChannelEvents, data).Err(); err != nil {
			time.Sleep(time.Second)
			continue
		}
		if _, err := readMessage(c, 2*time.Second); err == nil {
			recovered = true
			break
		}
	}

	if !recovered {
		t.Fatalf("❌ el subscriber NO reanudó la entrega tras reiniciar Redis")
	}
	t.Logf("✅ Resiliencia OK: el WS reconectó a Redis y reanudó la entrega tras la caída")
}
