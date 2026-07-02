package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

const testSecret = "test-secret-de-al-menos-32-caracteres-1234567890"

// --- Helpers ---------------------------------------------------------------

func testConfig() *Config {
	return &Config{
		JWTSecret:      testSecret,
		RedisHost:      "localhost",
		RedisPort:      "6379",
		RedisDB:        0,
		AllowedOrigins: []string{}, // sin Origin (clientes nativos) permitido
		Environment:    "test",
	}
}

func makeToken(t *testing.T, userID, userType, empresaID string) string {
	t.Helper()
	claims := JWTClaims{
		UserID:    userID,
		UserType:  userType,
		EmpresaID: empresaID,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	s, err := tok.SignedString([]byte(testSecret))
	if err != nil {
		t.Fatalf("firmando token: %v", err)
	}
	return s
}

// setupServer arranca hub + subscriber + servidor HTTP reales contra el Redis
// de Docker. Devuelve la URL ws:// y una función de limpieza.
func setupServer(t *testing.T) (wsURL string, pub *redis.Client, cleanup func()) {
	t.Helper()
	cfg := testConfig()

	rdb, err := NewRedisClient(cfg)
	if err != nil {
		t.Skipf("Redis no disponible en localhost:6379 (%v). ¿docker run redis?", err)
	}

	hub := NewHub()
	go hub.Run()

	ctx, cancel := context.WithCancel(context.Background())
	sub := NewRedisSubscriber(rdb, hub)
	go sub.Run(ctx)

	handler := NewWSHandler(hub, cfg)
	mux := http.NewServeMux()
	mux.HandleFunc("/ws/connect", handler.HandleConnect)
	srv := httptest.NewServer(mux)

	wsURL = "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws/connect"

	// Dar tiempo a que el subscriber abra la suscripción a Redis.
	time.Sleep(300 * time.Millisecond)

	cleanup = func() {
		cancel()
		srv.Close()
		rdb.Close()
	}
	return wsURL, rdb, cleanup
}

func dial(t *testing.T, wsURL, token string) *websocket.Conn {
	t.Helper()
	h := http.Header{}
	h.Set("Authorization", "Bearer "+token)
	c, resp, err := websocket.DefaultDialer.Dial(wsURL, h)
	if err != nil {
		code := 0
		if resp != nil {
			code = resp.StatusCode
		}
		t.Fatalf("dial WS falló (status %d): %v", code, err)
	}
	return c
}

func publishEvent(t *testing.T, pub *redis.Client, channel string, evt Event) {
	t.Helper()
	data, _ := json.Marshal(evt)
	if err := pub.Publish(context.Background(), channel, data).Err(); err != nil {
		t.Fatalf("publish falló: %v", err)
	}
}

// readMessage lee un mensaje con timeout.
func readMessage(c *websocket.Conn, timeout time.Duration) (Message, error) {
	c.SetReadDeadline(time.Now().Add(timeout))
	_, data, err := c.ReadMessage()
	if err != nil {
		return Message{}, err
	}
	var m Message
	err = json.Unmarshal(data, &m)
	return m, err
}

// --- Tests funcionales -----------------------------------------------------

func TestBroadcastToCliente(t *testing.T) {
	wsURL, pub, cleanup := setupServer(t)
	defer cleanup()

	token := makeToken(t, "cliente-1", "cliente", "")
	c := dial(t, wsURL, token)
	defer c.Close()
	time.Sleep(150 * time.Millisecond) // esperar registro en el hub

	publishEvent(t, pub, ChannelEvents, Event{
		Scope:    ScopeCliente,
		TargetID: "cliente-1",
		Message:  Message{Type: MessageTypeNotification, Timestamp: time.Now(), Data: map[string]interface{}{"msg": "hola"}},
	})

	m, err := readMessage(c, 3*time.Second)
	if err != nil {
		t.Fatalf("no se recibió el mensaje: %v", err)
	}
	if m.Type != MessageTypeNotification {
		t.Fatalf("tipo inesperado: %s", m.Type)
	}
	t.Logf("✅ Cliente recibió notificación correctamente")
}

func TestBroadcastToEmpresa(t *testing.T) {
	wsURL, pub, cleanup := setupServer(t)
	defer cleanup()

	token := makeToken(t, "user-emp", "empresa", "empresa-9")
	c := dial(t, wsURL, token)
	defer c.Close()
	time.Sleep(150 * time.Millisecond)

	publishEvent(t, pub, ChannelEvents, Event{
		Scope:    ScopeEmpresa,
		TargetID: "empresa-9",
		Message:  Message{Type: MessageTypeDeviceUpdate, Timestamp: time.Now(), Data: map[string]interface{}{"estado": "online"}},
	})

	m, err := readMessage(c, 3*time.Second)
	if err != nil {
		t.Fatalf("empresa no recibió el mensaje: %v", err)
	}
	if m.Type != MessageTypeDeviceUpdate {
		t.Fatalf("tipo inesperado: %s", m.Type)
	}
	t.Logf("✅ Empresa recibió device_update correctamente")
}

func TestAislamientoEntreClientes(t *testing.T) {
	wsURL, pub, cleanup := setupServer(t)
	defer cleanup()

	// cliente-A no debe recibir eventos dirigidos a cliente-B.
	tokA := makeToken(t, "cliente-A", "cliente", "")
	cA := dial(t, wsURL, tokA)
	defer cA.Close()
	time.Sleep(150 * time.Millisecond)

	publishEvent(t, pub, ChannelEvents, Event{
		Scope:    ScopeCliente,
		TargetID: "cliente-B",
		Message:  Message{Type: MessageTypeAlert, Timestamp: time.Now()},
	})

	_, err := readMessage(cA, 1*time.Second)
	if err == nil {
		t.Fatalf("❌ fuga: cliente-A recibió un evento de cliente-B")
	}
	t.Logf("✅ Aislamiento correcto: cliente-A NO recibió el evento de cliente-B")
}

// --- Test de carga: 250 conexiones simultáneas -----------------------------

func TestCarga250Conexiones(t *testing.T) {
	if testing.Short() {
		t.Skip("omitido en -short")
	}
	const N = 250
	wsURL, pub, cleanup := setupServer(t)
	defer cleanup()

	conns := make([]*websocket.Conn, 0, N)
	var connectFails int
	for i := 0; i < N; i++ {
		token := makeToken(t, fmt.Sprintf("cliente-%d", i), "cliente", "")
		h := http.Header{}
		h.Set("Authorization", "Bearer "+token)
		c, _, err := websocket.DefaultDialer.Dial(wsURL, h)
		if err != nil {
			connectFails++
			continue
		}
		conns = append(conns, c)
	}
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()
	t.Logf("Conexiones establecidas: %d/%d (fallos: %d)", len(conns), N, connectFails)
	time.Sleep(500 * time.Millisecond) // registro en el hub

	// Cada cliente escucha en paralelo.
	var received int64
	var maxLatency int64
	var wg sync.WaitGroup
	for _, c := range conns {
		wg.Add(1)
		go func(c *websocket.Conn) {
			defer wg.Done()
			m, err := readMessage(c, 5*time.Second)
			if err != nil {
				return
			}
			if sentStr, ok := m.Data["sentUnixNano"].(float64); ok {
				lat := time.Now().UnixNano() - int64(sentStr)
				for {
					cur := atomic.LoadInt64(&maxLatency)
					if lat <= cur || atomic.CompareAndSwapInt64(&maxLatency, cur, lat) {
						break
					}
				}
			}
			atomic.AddInt64(&received, 1)
		}(c)
	}

	start := time.Now()
	publishEvent(t, pub, ChannelBroadcast, Event{
		Scope:   ScopeAll,
		Message: Message{Type: MessageTypeConsumption, Timestamp: time.Now(), Data: map[string]interface{}{"sentUnixNano": float64(time.Now().UnixNano())}},
	})
	wg.Wait()
	elapsed := time.Since(start)

	rate := float64(received) / float64(len(conns)) * 100
	t.Logf("📊 Broadcast a %d clientes: %d recibieron (%.1f%%) en %v · latencia máx %.1f ms",
		len(conns), received, rate, elapsed, float64(atomic.LoadInt64(&maxLatency))/1e6)

	if rate < 99.0 {
		t.Fatalf("❌ tasa de entrega %.1f%% < 99%%", rate)
	}
	t.Logf("✅ Entrega ≥99%% a %d conexiones simultáneas", len(conns))
}
