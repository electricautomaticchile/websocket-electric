# syntax=docker/dockerfile:1

# ---- Builder ----
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Cachear dependencias primero.
COPY go.mod go.sum ./
RUN go mod download

# Compilar binario estático.
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build \
    -buildvcs=false \
    -ldflags '-s -w' \
    -o /websocket-electric .

# ---- Final (distroless, sin shell) ----
FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /
COPY --from=builder /websocket-electric /websocket-electric

# Puerto del WS Hub.
EXPOSE 8081

USER nonroot:nonroot
ENTRYPOINT ["/websocket-electric"]
