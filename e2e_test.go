package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// e2e_test.go valida el flujo completo end-to-end del sistema:
//
//	Backend/API  ──publish──▶  Redis Pub/Sub (ws:events)  ──▶  WS Hub  ──▶  Cliente WebSocket
//
// El objetivo es cubrir tanto la ENTREGA correcta como el AISLAMIENTO por
// empresa/cliente: un cliente jamás debe recibir mensajes destinados a otra
// empresa o a otro cliente.
//
// Reutiliza los helpers de integration_test.go (setupServer, makeToken, dial,
// publishEvent) y es CI-safe: si Redis no está disponible, setupServer hace
// t.Skip automáticamente.

// readFrame lee un frame WebSocket con timeout y lo divide en los mensajes
// individuales que lo componen. El WritePump del servidor agrupa varios
// mensajes en un mismo frame separándolos por '\n' (ver client.go), por lo que
// aquí se divide por '\n' y se hace json.Unmarshal de cada línea.
func readFrame(c *websocket.Conn, timeout time.Duration) ([]Message, error) {
	c.SetReadDeadline(time.Now().Add(timeout))
	_, data, err := c.ReadMessage()
	if err != nil {
		return nil, err
	}

	lines := strings.Split(string(data), "\n")
	msgs := make([]Message, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var m Message
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			return nil, err
		}
		msgs = append(msgs, m)
	}
	return msgs, nil
}

// TestE2EFlujoIoTPorEmpresaConAislamiento valida el flujo completo de un evento
// IoT (device_update) publicado por el backend en Redis hasta un cliente WS,
// verificando el aislamiento por empresa: solo la empresa destino lo recibe.
func TestE2EFlujoIoTPorEmpresaConAislamiento(t *testing.T) {
	wsURL, pub, cleanup := setupServer(t)
	defer cleanup()

	const empresaA = "empresa-A"
	const empresaB = "empresa-B"

	// Cliente WS autenticado como empresa A (el que DEBE recibir el evento).
	tokenA := makeToken(t, "user-empresa-A", "empresa", empresaA)
	connA := dial(t, wsURL, tokenA)
	defer connA.Close()

	// Cliente WS autenticado como empresa B (NO debe recibir nada).
	tokenB := makeToken(t, "user-empresa-B", "empresa", empresaB)
	connB := dial(t, wsURL, tokenB)
	defer connB.Close()

	// Esperar a que ambos clientes queden registrados en el Hub.
	time.Sleep(200 * time.Millisecond)

	// El backend publica un evento IoT dirigido SOLO a la empresa A.
	deviceData := map[string]interface{}{
		"deviceId": "medidor-123",
		"lat":      -33.4489,
		"lng":      -70.6693,
		"energia":  1234.56,
	}
	publishEvent(t, pub, ChannelEvents, Event{
		Scope:    ScopeEmpresa,
		TargetID: empresaA,
		Message: Message{
			Type:      MessageTypeDeviceUpdate,
			Timestamp: time.Now(),
			EmpresaID: empresaA,
			Data:      deviceData,
		},
	})

	// La empresa A DEBE recibir el device_update con los datos correctos.
	msgs, err := readFrame(connA, 3*time.Second)
	if err != nil {
		t.Fatalf("empresa A no recibió el device_update: %v", err)
	}

	var found *Message
	for i := range msgs {
		if msgs[i].Type == MessageTypeDeviceUpdate {
			found = &msgs[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("empresa A no recibió un mensaje device_update; recibió: %+v", msgs)
	}
	if found.EmpresaID != empresaA {
		t.Fatalf("empresaId inesperado: esperaba %q, obtuvo %q", empresaA, found.EmpresaID)
	}
	if got, ok := found.Data["deviceId"].(string); !ok || got != "medidor-123" {
		t.Fatalf("deviceId inesperado en el payload: %+v", found.Data)
	}
	if got, ok := found.Data["energia"].(float64); !ok || got != 1234.56 {
		t.Fatalf("energia inesperada en el payload: %+v", found.Data)
	}
	t.Logf("✅ Empresa A recibió el device_update correcto (deviceId=medidor-123, energia=1234.56)")

	// La empresa B NO debe recibir nada: timeout corto esperado.
	if _, err := readFrame(connB, 1*time.Second); err == nil {
		t.Fatalf("❌ fuga de aislamiento: la empresa B recibió un evento destinado a la empresa A")
	}
	t.Logf("✅ Aislamiento por empresa OK: la empresa B NO recibió el evento de la empresa A")
}

// TestE2EFlujoPorClienteConAislamiento replica el flujo end-to-end pero para el
// scope "cliente" (TargetID = clienteId). Verifica que solo el cliente destino
// recibe el evento y que otro cliente distinto no recibe nada.
func TestE2EFlujoPorClienteConAislamiento(t *testing.T) {
	wsURL, pub, cleanup := setupServer(t)
	defer cleanup()

	const clienteA = "cliente-A"
	const clienteB = "cliente-B"

	// Ambos clientes son de tipo "cliente"; el Hub los indexa por UserID.
	tokenA := makeToken(t, clienteA, "cliente", "")
	connA := dial(t, wsURL, tokenA)
	defer connA.Close()

	tokenB := makeToken(t, clienteB, "cliente", "")
	connB := dial(t, wsURL, tokenB)
	defer connB.Close()

	time.Sleep(200 * time.Millisecond)

	// El backend publica una alerta dirigida SOLO al cliente A.
	publishEvent(t, pub, ChannelEvents, Event{
		Scope:    ScopeCliente,
		TargetID: clienteA,
		Message: Message{
			Type:      MessageTypeAlert,
			Timestamp: time.Now(),
			ClienteID: clienteA,
			Data:      map[string]interface{}{"nivel": "critico", "mensaje": "consumo elevado"},
		},
	})

	// El cliente A DEBE recibir la alerta.
	msgs, err := readFrame(connA, 3*time.Second)
	if err != nil {
		t.Fatalf("cliente A no recibió la alerta: %v", err)
	}
	var found *Message
	for i := range msgs {
		if msgs[i].Type == MessageTypeAlert {
			found = &msgs[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("cliente A no recibió un mensaje alert; recibió: %+v", msgs)
	}
	if found.ClienteID != clienteA {
		t.Fatalf("clienteId inesperado: esperaba %q, obtuvo %q", clienteA, found.ClienteID)
	}
	if got, ok := found.Data["nivel"].(string); !ok || got != "critico" {
		t.Fatalf("nivel inesperado en el payload: %+v", found.Data)
	}
	t.Logf("✅ Cliente A recibió la alerta correcta")

	// El cliente B NO debe recibir nada.
	if _, err := readFrame(connB, 1*time.Second); err == nil {
		t.Fatalf("❌ fuga de aislamiento: el cliente B recibió un evento destinado al cliente A")
	}
	t.Logf("✅ Aislamiento por cliente OK: el cliente B NO recibió el evento del cliente A")
}
