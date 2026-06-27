package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/gorilla/websocket"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 512 * 1024
)

// Client representa una conexión WebSocket de un usuario autenticado.
type Client struct {
	Hub       *Hub
	Conn      *websocket.Conn
	Send      chan []byte
	UserID    string
	UserType  string
	EmpresaID string
}

// ReadPump lee mensajes entrantes del cliente y mantiene vivo el deadline de
// lectura mediante el pong handler. Al terminar, desregistra el cliente.
func (c *Client) ReadPump() {
	defer func() {
		c.Hub.Unregister <- c
		c.Conn.Close()
	}()

	c.Conn.SetReadLimit(maxMessageSize)
	c.Conn.SetReadDeadline(time.Now().Add(pongWait))
	c.Conn.SetPongHandler(func(string) error {
		c.Conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, message, err := c.Conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("⚠️ error de lectura: %v", err)
			}
			break
		}

		var clientMsg ClientMessage
		if err := json.Unmarshal(message, &clientMsg); err != nil {
			log.Printf("⚠️ Error parseando mensaje del cliente: %v", err)
			continue
		}

		c.handleClientMessage(clientMsg)
	}
}

// WritePump envía mensajes salientes al cliente y emite pings periódicos para
// detectar conexiones muertas (heartbeat).
func (c *Client) WritePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.Conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.Send:
			c.Conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.Conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			w.Write(message)

			// Vaciar la cola pendiente en el mismo frame para reducir syscalls.
			n := len(c.Send)
			for i := 0; i < n; i++ {
				w.Write([]byte{'\n'})
				w.Write(<-c.Send)
			}

			if err := w.Close(); err != nil {
				return
			}

		case <-ticker.C:
			c.Conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (c *Client) handleClientMessage(msg ClientMessage) {
	switch msg.Type {
	case "ping":
		c.sendPong()
	case "subscribe":
		log.Printf("Cliente %s suscrito a: %v", c.UserID, msg.Data)
	default:
		log.Printf("Tipo de mensaje desconocido: %s", msg.Type)
	}
}

func (c *Client) sendPong() {
	pongMsg := Message{
		Type:      MessageTypePong,
		Timestamp: time.Now(),
	}
	data, _ := json.Marshal(pongMsg)
	select {
	case c.Send <- data:
	default:
	}
}
