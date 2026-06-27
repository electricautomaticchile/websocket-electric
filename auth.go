package main

import (
	"fmt"

	"github.com/golang-jwt/jwt/v5"
)

// JWTClaims replica exactamente los claims que emite y valida la API REST,
// de modo que el WS valida el mismo token con la misma clave secreta.
type JWTClaims struct {
	UserID    string   `json:"userId"`
	UserRole  string   `json:"userRole"`
	UserType  string   `json:"userType"`
	EmpresaID string   `json:"empresaId,omitempty"`
	Powers    []string `json:"powers,omitempty"`
	jwt.RegisteredClaims
}

// ParseJWTClaims valida la firma y expiración del token y devuelve los claims.
// Usa HS256 y la misma JWT_SECRET que la API.
func ParseJWTClaims(tokenString, secret string) (*JWTClaims, error) {
	token, err := jwt.ParseWithClaims(tokenString, &JWTClaims{}, func(token *jwt.Token) (interface{}, error) {
		if token.Method != jwt.SigningMethodHS256 {
			return nil, fmt.Errorf("método de firma inválido")
		}
		return []byte(secret), nil
	})
	if err != nil || !token.Valid {
		return nil, fmt.Errorf("token inválido")
	}

	claims, ok := token.Claims.(*JWTClaims)
	if !ok {
		return nil, fmt.Errorf("claims inválidos")
	}

	return claims, nil
}
