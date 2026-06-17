# Graceful Shutdown

Sources: [VictoriaMetrics Blog - Graceful Shutdown in Go](https://victoriametrics.com/blog/go-graceful-shutdown/), [Go 1.16 Release Notes](https://go.dev/doc/go1.16) (signal.NotifyContext), [net/http documentation](https://pkg.go.dev/net/http#Server.Shutdown)

---

## The Production Pattern

A Go service should shut down in order: stop accepting new work, drain in-flight work, close resources.

```go
func main() {
    cfg := config.MustLoad()
    logger, _ := zap.NewProduction()

    // Wire dependencies
    redisClient := redis.NewClient(cfg.Redis)
    producer := kafka.NewProducer(cfg.Kafka)
    engine := routing.NewEngine(redisClient, logger)
    eslClient := esl.NewClient(cfg.FreeSWITCH, logger)

    // HTTP server
    handler := handler.NewDialplan(engine, producer, logger)
    srv := &http.Server{
        Addr:    ":" + cfg.AppPort,
        Handler: handler.Router(),
    }

    // --- Start phase ---

    // Start ESL client in background
    go eslClient.Connect(context.Background())

    // Start HTTP server in background
    go func() {
        logger.Info("HTTP server starting", zap.String("addr", srv.Addr))
        if err := srv.ListenAndServe(); err != http.ErrServerClosed {
            logger.Fatal("HTTP server error", zap.Error(err))
        }
    }()

    // --- Wait for shutdown signal ---

    // signal.NotifyContext (Go 1.16+) ties OS signals to context cancellation
    ctx, stop := signal.NotifyContext(context.Background(),
        syscall.SIGINT,  // Ctrl+C
        syscall.SIGTERM, // Docker stop, Kubernetes pod termination
    )
    defer stop()

    <-ctx.Done() // blocks until signal received
    logger.Info("shutdown signal received")

    // --- Shutdown phase (ordered) ---

    // 1. Stop accepting new HTTP connections (drain in-flight requests)
    shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
    defer cancel()

    if err := srv.Shutdown(shutdownCtx); err != nil {
        logger.Error("HTTP server shutdown error", zap.Error(err))
    }
    logger.Info("HTTP server stopped")

    // 2. Disconnect ESL client (stop receiving events)
    eslClient.Close()
    logger.Info("ESL client disconnected")

    // 3. Flush Kafka producer (ensure buffered messages are sent)
    producer.Close()
    logger.Info("Kafka producer flushed")

    // 4. Close Redis connection (last — other components may need it during drain)
    redisClient.Close()
    logger.Info("Redis connection closed")

    logger.Info("shutdown complete")
}
```

---

## Why signal.NotifyContext

Before Go 1.16, you had to manually wire signal channels:

```go
// OLD way (pre-1.16) — more boilerplate
sigCh := make(chan os.Signal, 1)
signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
<-sigCh

// NEW way (Go 1.16+) — integrates with context
ctx, stop := signal.NotifyContext(context.Background(),
    syscall.SIGINT, syscall.SIGTERM)
defer stop()
<-ctx.Done()
```

**Why the new way is better:** The context can be passed to any function that accepts `context.Context`. When the signal fires, EVERYTHING downstream that's using this context gets cancelled automatically — HTTP handlers, Redis calls, Kafka produces. No manual plumbing.

---

## Why Shutdown Order Matters

```
WRONG ORDER:
  1. Close Redis       ← routing handler tries to read Redis → crash
  2. Close Kafka       ← ESL handler tries to produce → error
  3. Stop HTTP server  ← in-flight requests get 500s
  4. Stop ESL          ← events lost

RIGHT ORDER:
  1. Stop HTTP server  ← stop accepting NEW calls, drain existing
  2. Stop ESL client   ← stop receiving events
  3. Close Kafka       ← flush remaining messages
  4. Close Redis       ← nothing else needs it
```

**Why:** Dependencies form a DAG. Close consumers before producers, close clients before the resources they depend on. HTTP handlers depend on Redis and Kafka. ESL handlers depend on Redis and Kafka. So HTTP and ESL stop first, then Kafka, then Redis.

---

## http.Server.Shutdown Behavior

From the Go standard library docs: "Shutdown gracefully shuts down the server without interrupting any active connections. Shutdown works by first closing all open listeners, then closing all idle connections, and then waiting indefinitely for connections to return to idle and then shut down."

```go
// server.Shutdown does:
// 1. Closes the listener (no new connections)
// 2. Closes idle connections immediately
// 3. Waits for active connections to finish (up to context deadline)
// 4. Returns nil if all drained, or ctx.Err() if timed out

shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
defer cancel()

err := srv.Shutdown(shutdownCtx)
// err == nil: all requests completed
// err == context.DeadlineExceeded: timed out, some requests were killed
```

**Why 15 seconds:** Long enough for most in-flight routing decisions to complete (they take <50ms typically). Short enough to not block container restarts. Kubernetes default `terminationGracePeriodSeconds` is 30s — leave headroom.

---

## ESL Reconnect with Backoff

The ESL client must reconnect if FreeSWITCH restarts. Use exponential backoff:

```go
func (c *ESLClient) Connect(ctx context.Context) {
    backoff := 1 * time.Second
    maxBackoff := 60 * time.Second

    for {
        select {
        case <-ctx.Done():
            return // shutdown requested
        default:
        }

        err := c.connect(ctx)
        if err == nil {
            backoff = 1 * time.Second // reset on success
            c.handleEvents(ctx)       // blocks until disconnected
        }

        c.logger.Warn("ESL connection lost, reconnecting",
            zap.Error(err),
            zap.Duration("backoff", backoff),
        )

        select {
        case <-time.After(backoff):
        case <-ctx.Done():
            return
        }

        // Exponential backoff: 1s → 2s → 4s → 8s → ... → 60s max
        backoff *= 2
        if backoff > maxBackoff {
            backoff = maxBackoff
        }
    }
}
```

**Why exponential backoff:** If FreeSWITCH is restarting, hammering it with connection attempts every 100ms makes things worse. Backing off gives it time to come up. Capping at 60s ensures we reconnect within a minute of it being healthy again.
