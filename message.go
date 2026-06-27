package main

import "time"

// MessageType identifica el tipo de mensaje enviado a los clientes WebSocket.
type MessageType string

const (
	MessageTypeAlert        MessageType = "alert"
	MessageTypeNotification MessageType = "notification"
	MessageTypeDeviceUpdate MessageType = "device_update"
	MessageTypeConsumption  MessageType = "consumption"
	MessageTypePing         MessageType = "ping"
	MessageTypePong         MessageType = "pong"
)

// Message es la estructura que se serializa y se envía a los clientes WebSocket.
// Es idéntica al contrato que la API publica en Redis Pub/Sub, de modo que el
// payload puede reenviarse sin transformación.
type Message struct {
	Type      MessageType            `json:"type"`
	Data      map[string]interface{} `json:"data"`
	Timestamp time.Time              `json:"timestamp"`
	EmpresaID string                 `json:"empresaId,omitempty"`
	ClienteID string                 `json:"clienteId,omitempty"`
}

// ClientMessage representa los mensajes entrantes desde el cliente WebSocket.
type ClientMessage struct {
	Type   string                 `json:"type"`
	Action string                 `json:"action,omitempty"`
	Data   map[string]interface{} `json:"data,omitempty"`
}

// Event es el sobre que viaja por Redis Pub/Sub entre la API y el WS Hub.
// La API publica un Event en un canal y el Hub lo enruta a los clientes.
type Event struct {
	// Scope define el destino del broadcast: "cliente", "empresa" o "all".
	Scope string `json:"scope"`
	// TargetID es el ID del cliente o empresa destino (vacío cuando Scope == "all").
	TargetID string `json:"targetId,omitempty"`
	// Message es el mensaje a entregar a los clientes WebSocket.
	Message Message `json:"message"`
}

// Constantes de scope para los eventos publicados en Redis.
const (
	ScopeCliente = "cliente"
	ScopeEmpresa = "empresa"
	ScopeAll     = "all"
)
