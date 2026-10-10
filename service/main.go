package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"frontier/service/storage"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	startup, cancel := context.WithTimeout(ctx, 30*time.Second)
	s, err := storage.Open(startup, databaseURL())
	cancel()
	if err != nil {
		slog.Error("database startup failed", "error", err)
		os.Exit(1)
	}
	defer s.Close()
	if os.Getenv("AUTH_TOKEN") == "" || os.Getenv("KINESIS_STREAM_NAME") == "" {
		slog.Error("AUTH_TOKEN and KINESIS_STREAM_NAME are required")
		os.Exit(1)
	}
	awsConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		slog.Error("AWS configuration failed", "error", err)
		os.Exit(1)
	}
	stream := &stream{client: kinesis.NewFromConfig(awsConfig), name: os.Getenv("KINESIS_STREAM_NAME"), store: s}
	if os.Getenv("SERVICE_ROLE") == "worker" {
		slog.Info("Kinesis worker starting", "stream", stream.name)
		if err = stream.worker(ctx); err != nil {
			slog.Error("worker failed", "error", err)
			os.Exit(1)
		}
		return
	}
	if role := os.Getenv("SERVICE_ROLE"); role != "" && role != "api" {
		slog.Error("unknown service role", "role", role)
		os.Exit(1)
	}
	go stream.publisher(ctx)
	handler := &api{store: s, token: os.Getenv("AUTH_TOKEN"), tenant: storage.Hash(os.Getenv("AUTH_TOKEN"))}
	server := &http.Server{Addr: ":8080", Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			slog.Error("shutdown failed", "error", err)
		}
	}()
	slog.Info("API listening", "address", server.Addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("server failed", "error", err)
		os.Exit(1)
	}
}

type api struct {
	store         *storage.Store
	token, tenant string
}

func write(w http.ResponseWriter, res result) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(res.Status)
	_, _ = w.Write(res.Body)
}

func (a *api) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 4500*time.Millisecond)
	defer cancel()
	// ALB readiness is separate from the authenticated compatibility endpoint.
	if r.Method == "GET" && r.URL.Path == "/ready" {
		if err := a.store.Ping(ctx); err != nil {
			write(w, failure(503, "E503", "servicio no disponible"))
		} else {
			write(w, response(200, map[string]bool{"ok": true}))
		}
		return
	}
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+a.token)) != 1 {
		write(w, failure(401, "E001", "no autorizado"))
		return
	}
	switch {
	case r.Method == "GET" && r.URL.Path == "/":
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("hello world\n"))
	case r.Method == "GET" && r.URL.Path == "/health":
		if err := a.store.Ping(ctx); err != nil {
			a.internalError(w, err)
		} else {
			write(w, response(200, map[string]bool{"ok": true}))
		}
	case r.Method == "POST" && r.URL.Path == "/__reset":
		if err := a.store.Reset(ctx, a.tenant); err != nil {
			a.internalError(w, err)
		} else {
			write(w, response(200, map[string]bool{"ok": true}))
		}
	case r.Method == "POST" && r.URL.Path == "/msg":
		a.message(ctx, w, r)
	default:
		write(w, failure(404, "E404", "ruta desconocida"))
	}
}

func (a *api) internalError(w http.ResponseWriter, err error) {
	slog.Error("request failed", "error", err)
	write(w, failure(503, "E503", "servicio no disponible"))
}

func (a *api) message(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64*1024))
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		write(w, failure(400, "E100", "json inválido"))
		return
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		write(w, failure(400, "E100", "json inválido"))
		return
	}
	body, _ := decoded.(map[string]any)
	m, ok := body["m"].(string)
	if !ok {
		write(w, failure(400, "E102", "falta m"))
		return
	}
	c, invalid := parseMessage(m)
	if invalid != nil {
		write(w, *invalid)
		return
	}
	switch c.Kind {
	case "PING":
		write(w, response(200, map[string]bool{"pong": true}))
		return
	case "ALTA":
		account, err := a.store.CreateAccount(ctx, a.tenant, c.Args[0])
		if err != nil {
			a.internalError(w, err)
		} else {
			write(w, response(201, map[string]string{"cuentaId": account.ID}))
		}
		return
	case "CONSULTA":
		account, err := a.store.GetAccount(ctx, a.tenant, c.Args[0])
		if errors.Is(err, storage.ErrAccountNotFound) {
			write(w, failure(404, "E401", "cuenta inexistente"))
		} else if err != nil {
			a.internalError(w, err)
		} else {
			write(w, response(200, map[string]any{"cuenta_id": account.ID, "saldo": account.Balance}))
		}
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if len(key) > 128 {
		write(w, failure(400, "E105", "clave de idempotencia inválida"))
		return
	}
	var id string
	var err error
	switch c.Kind {
	case "DEPOSITO":
		id, err = a.store.QueueDeposit(ctx, a.tenant, key, c.Raw, c.Args[0], c.Amount)
	case "TRANSFER":
		id, err = a.store.QueueTransfer(ctx, a.tenant, key, c.Raw, c.Args[0], c.Args[1], c.Amount)
	case "ANULA":
		id, err = a.store.QueueCancellation(ctx, a.tenant, key, c.Raw, c.Args[0])
	}
	if errors.Is(err, storage.ErrIdempotencyConflict) {
		write(w, failure(409, "E409", "clave de idempotencia reutilizada"))
		return
	}
	if err != nil {
		a.internalError(w, err)
		return
	}
	w.Header().Set("X-Request-ID", id)
	for ctx.Err() == nil {
		res, err := a.store.OperationResult(ctx, id, a.tenant)
		if err != nil && ctx.Err() == nil {
			a.internalError(w, err)
			return
		}
		if res != nil {
			write(w, *res)
			return
		}
		if !pause(ctx, 40*time.Millisecond) {
			break
		}
	}
	// A timeout is not a failed operation: it can still complete. A caller that
	// supplied Idempotency-Key can safely retry and receive the stored outcome.
	write(w, response(503, map[string]string{"error": "E503", "mensaje": "operación pendiente", "request_id": id}))
}
