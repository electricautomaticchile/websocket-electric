// Command iotsim simula N dispositivos IoT enviando lecturas en tiempo real y
// mide el throughput y la latencia del camino API→Redis→WS→dashboards.
//
// Modela la operación 24/7: cada "dispositivo" publica un device_update en
// Redis (como haría la API tras ingerir la lectura del ESP32), y unos
// "dashboards" (clientes WS de empresa) reciben el broadcast.
//
// Uso:
//   go run ./cmd/iotsim -devices 100 -dashboards 3 -interval 1s -duration 20s \
//     -ws ws://127.0.0.1:39217/ws/connect -redis localhost:6379 \
//     -secret test-secret-de-al-menos-32-caracteres-1234567890
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

const empresaID = "empresa-sim"

func main() {
	devices := flag.Int("devices", 100, "número de dispositivos simulados")
	dashboards := flag.Int("dashboards", 3, "clientes WS de empresa conectados")
	interval := flag.Duration("interval", time.Second, "intervalo entre lecturas por dispositivo")
	duration := flag.Duration("duration", 20*time.Second, "duración de la simulación")
	wsURL := flag.String("ws", "ws://127.0.0.1:39217/ws/connect", "URL del WS")
	redisAddr := flag.String("redis", "localhost:6379", "dirección de Redis")
	secret := flag.String("secret", "test-secret-de-al-menos-32-caracteres-1234567890", "JWT secret")
	flag.Parse()

	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("Redis no disponible: %v", err)
	}
	defer rdb.Close()

	token := makeToken(*secret)

	var delivered int64
	var latSumNs int64
	var latMaxNs int64

	// Conectar dashboards (empresa).
	var dashConns []*websocket.Conn
	for i := 0; i < *dashboards; i++ {
		h := http.Header{}
		h.Set("Authorization", "Bearer "+token)
		c, _, err := websocket.DefaultDialer.Dial(*wsURL, h)
		if err != nil {
			log.Fatalf("dashboard %d no conectó: %v", i, err)
		}
		dashConns = append(dashConns, c)
		go func(c *websocket.Conn) {
			for {
				_, data, err := c.ReadMessage()
				if err != nil {
					return
				}
				// El WritePump del hub agrupa varios mensajes en un frame
				// separados por '\n'. Hay que dividir antes de parsear.
				for _, line := range splitLines(data) {
					var m map[string]interface{}
					if json.Unmarshal(line, &m) == nil {
						if d, ok := m["data"].(map[string]interface{}); ok {
							if sent, ok := d["sentUnixNano"].(float64); ok {
								lat := time.Now().UnixNano() - int64(sent)
								atomic.AddInt64(&latSumNs, lat)
								for {
									cur := atomic.LoadInt64(&latMaxNs)
									if lat <= cur || atomic.CompareAndSwapInt64(&latMaxNs, cur, lat) {
										break
									}
								}
							}
						}
						atomic.AddInt64(&delivered, 1)
					}
				}
			}
		}(c)
	}
	time.Sleep(500 * time.Millisecond)
	fmt.Printf("Dashboards conectados: %d\n", len(dashConns))

	// Lanzar dispositivos.
	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()
	var published int64
	var wg sync.WaitGroup
	start := time.Now()

	for d := 0; d < *devices; d++ {
		wg.Add(1)
		go func(deviceID int) {
			defer wg.Done()
			ticker := time.NewTicker(*interval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					evt := map[string]interface{}{
						"scope":    "empresa",
						"targetId": empresaID,
						"message": map[string]interface{}{
							"type":      "device_update",
							"timestamp": time.Now().Format(time.RFC3339),
							"empresaId": empresaID,
							"data": map[string]interface{}{
								"idDispositivo":  fmt.Sprintf("dev-%d", deviceID),
								"potenciaActiva": 1200 + deviceID,
								"voltaje":        220,
								"corriente":      5.4,
								"sentUnixNano":   float64(time.Now().UnixNano()),
							},
						},
					}
					data, _ := json.Marshal(evt)
					if rdb.Publish(context.Background(), "ws:events", data).Err() == nil {
						atomic.AddInt64(&published, 1)
					}
				}
			}
		}(d)
	}

	wg.Wait()
	time.Sleep(500 * time.Millisecond) // drenar entregas en vuelo
	elapsed := time.Since(start)

	for _, c := range dashConns {
		c.Close()
	}

	pub := atomic.LoadInt64(&published)
	del := atomic.LoadInt64(&delivered)
	esperados := pub * int64(len(dashConns))
	var latAvgMs, latMaxMs float64
	if del > 0 {
		latAvgMs = float64(atomic.LoadInt64(&latSumNs)) / float64(del) / 1e6
		latMaxMs = float64(atomic.LoadInt64(&latMaxNs)) / 1e6
	}

	fmt.Println("\n================ RESULTADO SIMULACIÓN IoT ================")
	fmt.Printf("Dispositivos:            %d\n", *devices)
	fmt.Printf("Intervalo por lectura:   %v  (real: 30s → factor %.0fx)\n", *interval, float64(30*time.Second)/float64(*interval))
	fmt.Printf("Duración:                %v\n", elapsed.Round(time.Millisecond))
	fmt.Printf("Eventos publicados:      %d  (%.1f/s)\n", pub, float64(pub)/elapsed.Seconds())
	fmt.Printf("Entregas a dashboards:   %d de %d esperadas (%.1f%%)\n", del, esperados, float64(del)/float64(esperados)*100)
	fmt.Printf("Latencia entrega:        avg %.2f ms · máx %.2f ms\n", latAvgMs, latMaxMs)
	fmt.Println("==========================================================")
}

func splitLines(data []byte) [][]byte {
	var out [][]byte
	start := 0
	for i := 0; i < len(data); i++ {
		if data[i] == '\n' {
			if i > start {
				out = append(out, data[start:i])
			}
			start = i + 1
		}
	}
	if start < len(data) {
		out = append(out, data[start:])
	}
	return out
}

func makeToken(secret string) string {
	claims := jwt.MapClaims{
		"userId":    "dashboard-sim",
		"userType":  "empresa",
		"empresaId": empresaID,
		"exp":       time.Now().Add(time.Hour).Unix(),
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	s, _ := tok.SignedString([]byte(secret))
	return s
}
