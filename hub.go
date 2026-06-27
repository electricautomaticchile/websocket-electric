package main

import (
	"encoding/json"
	"log"
	"sync"
)

// Hub mantiene el estado de los clientes WebSocket conectados y enruta los
// broadcasts. El acceso al estado se protege con un RWMutex; los canales
// Register/Unregister/Broadcast serializan las mutaciones en Run().
type Hub struct {
	Clients    map[*Client]bool
	Broadcast  chan []byte
	Register   chan *Client
	Unregister chan *Client

	empresaClients map[string]map[*Client]bool
	clienteClients map[string]map[*Client]bool
	mu             sync.RWMutex
}

func NewHub() *Hub {
	return &Hub{
		Broadcast:      make(chan []byte, 256),
		Register:       make(chan *Client),
		Unregister:     make(chan *Client),
		Clients:        make(map[*Client]bool),
		empresaClients: make(map[string]map[*Client]bool),
		clienteClients: make(map[string]map[*Client]bool),
	}
}

// Run procesa registros, desregistros y broadcasts globales en una sola
// goroutine, evitando condiciones de carrera sobre los mapas de clientes.
func (h *Hub) Run() {
	for {
		select {
		case client := <-h.Register:
			h.mu.Lock()
			h.Clients[client] = true

			if client.UserType == "empresa" && client.EmpresaID != "" {
				if h.empresaClients[client.EmpresaID] == nil {
					h.empresaClients[client.EmpresaID] = make(map[*Client]bool)
				}
				h.empresaClients[client.EmpresaID][client] = true
			} else if client.UserType == "cliente" && client.UserID != "" {
				if h.clienteClients[client.UserID] == nil {
					h.clienteClients[client.UserID] = make(map[*Client]bool)
				}
				h.clienteClients[client.UserID][client] = true
			}
			h.mu.Unlock()

			log.Printf("🔌 Cliente conectado: %s (tipo: %s)", client.UserID, client.UserType)

		case client := <-h.Unregister:
			h.mu.Lock()
			h.unregisterLocked(client)
			h.mu.Unlock()

			log.Printf("🔌 Cliente desconectado: %s", client.UserID)

		case message := <-h.Broadcast:
			h.mu.Lock()
			for client := range h.Clients {
				select {
				case client.Send <- message:
				default:
					h.unregisterLocked(client)
				}
			}
			h.mu.Unlock()
		}
	}
}

// unregisterLocked elimina un cliente de todos los índices. El llamador debe
// tener el lock de escritura tomado.
func (h *Hub) unregisterLocked(client *Client) {
	if _, ok := h.Clients[client]; !ok {
		return
	}

	delete(h.Clients, client)
	close(client.Send)

	if client.UserType == "empresa" && client.EmpresaID != "" {
		if clients, ok := h.empresaClients[client.EmpresaID]; ok {
			delete(clients, client)
			if len(clients) == 0 {
				delete(h.empresaClients, client.EmpresaID)
			}
		}
	} else if client.UserType == "cliente" && client.UserID != "" {
		if clients, ok := h.clienteClients[client.UserID]; ok {
			delete(clients, client)
			if len(clients) == 0 {
				delete(h.clienteClients, client.UserID)
			}
		}
	}
}

// BroadcastToAll envía un mensaje a todos los clientes conectados.
func (h *Hub) BroadcastToAll(msg Message) {
	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("⚠️ Error serializando mensaje: %v", err)
		return
	}
	h.Broadcast <- data
}

// BroadcastToEmpresa envía un mensaje a todos los clientes de una empresa.
func (h *Hub) BroadcastToEmpresa(empresaID string, msg Message) {
	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("⚠️ Error serializando mensaje: %v", err)
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	clients, ok := h.empresaClients[empresaID]
	if !ok {
		return
	}

	for client := range clients {
		select {
		case client.Send <- data:
		default:
			h.unregisterLocked(client)
		}
	}
}

// BroadcastToCliente envía un mensaje a todos los clientes de un cliente.
func (h *Hub) BroadcastToCliente(clienteID string, msg Message) {
	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("⚠️ Error serializando mensaje: %v", err)
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	clients, ok := h.clienteClients[clienteID]
	if !ok {
		return
	}

	for client := range clients {
		select {
		case client.Send <- data:
		default:
			h.unregisterLocked(client)
		}
	}
}

// GetConnectedClients devuelve el número de clientes conectados.
func (h *Hub) GetConnectedClients() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.Clients)
}
