# Project Structure

Sources: [Effective Go](https://go.dev/doc/effective_go), [Google Go Style Guide](https://google.github.io/styleguide/go/best-practices.html), [Go Code Review Comments](https://go.dev/wiki/CodeReviewComments)

---

## The cmd/ and internal/ Convention

This is the standard Go project layout used by the Go standard library, Kubernetes, Docker, and most production Go projects.

```
project-root/
├── cmd/
│   └── router/
│       └── main.go           # Entry point — wires dependencies, starts server
├── internal/                 # ENFORCED by Go compiler — external modules cannot import
│   ├── config/
│   │   └── config.go         # Env var parsing into typed Config struct
│   ├── handler/
│   │   └── dialplan.go       # HTTP handler — thin, delegates to service layer
│   ├── routing/
│   │   ├── engine.go         # Core business logic
│   │   ├── strategy.go       # Strategy implementations
│   │   └── availability.go   # Filter chain
│   ├── redis/
│   │   ├── client.go         # Redis connection setup
│   │   └── lua.go            # Lua script helpers
│   ├── kafka/
│   │   └── producer.go       # Kafka producer wrapper
│   ├── esl/
│   │   └── client.go         # FreeSWITCH ESL client
│   └── models/
│       └── types.go          # Domain types (Campaign, Target, etc.)
├── go.mod                    # Module definition + dependencies
├── go.sum                    # Dependency checksums (committed to git)
├── .env.example              # Environment variable template
└── CLAUDE.md
```

**Why `cmd/`:** Separates the entry point (wiring, configuration) from business logic. A project can have multiple binaries (`cmd/router/`, `cmd/migrate/`, `cmd/healthcheck/`) sharing the same `internal/` packages.

**Why `internal/`:** Go enforces that code in `internal/` cannot be imported by modules outside the parent. This is a compiler-level guarantee — not a convention. It protects implementation details from leaking into your public API.

---

## Package Design Principles

### One Package = One Responsibility

From Go Code Review Comments: "Avoid meaningless package names like util, common, misc."

```go
// BAD — what does "util" do?
package util

func FormatPhone(number string) string { ... }
func ParseTime(s string) (time.Time, error) { ... }
func HashPassword(pw string) (string, error) { ... }

// GOOD — clear single responsibilities
package phone    // phone.Format(number)
package timeutil // timeutil.Parse(s)  (if stdlib time isn't enough)
package auth     // auth.HashPassword(pw)
```

**Why:** A well-named package tells you what it does before you read a line of code. `redis.NewClient()` is self-documenting. `util.NewClient()` is meaningless.

### Don't Organize by Technical Layer

```go
// BAD — organized by technical type
internal/
├── models/       // all models dumped here
├── handlers/     // all handlers dumped here
├── services/     // all services dumped here
└── repositories/ // all repos dumped here

// GOOD — organized by domain
internal/
├── routing/      // everything about routing decisions
├── dialplan/     // everything about XML generation
├── redis/        // Redis client and Lua scripts
├── kafka/        // Kafka producer
└── esl/          // ESL event handling
```

**Why:** When you need to change how routing works, you go to `internal/routing/`. You don't have to open 4 different directories to find the model, the handler, the service, and the repository for routing. Domain-organized code has higher cohesion and lower coupling.

---

## The main.go Pattern

`cmd/router/main.go` should do exactly three things: load config, wire dependencies, start servers.

```go
package main

import (
    "context"
    "os"
    "os/signal"
    "syscall"

    "go.uber.org/zap"

    "github.com/sageteck/dial-plan-router/internal/config"
    "github.com/sageteck/dial-plan-router/internal/handler"
    "github.com/sageteck/dial-plan-router/internal/kafka"
    "github.com/sageteck/dial-plan-router/internal/redis"
)

func main() {
    // 1. Load config
    cfg, err := config.Load()
    if err != nil {
        // OK to fatal here — can't run without config
        log.Fatalf("loading config: %v", err)
    }

    // 2. Wire dependencies (dependency injection via constructors)
    logger, _ := zap.NewProduction()
    redisClient := redis.NewClient(cfg.Redis)
    producer := kafka.NewProducer(cfg.Kafka)
    engine := routing.NewEngine(redisClient, logger)
    h := handler.NewDialplan(engine, producer, logger)

    // 3. Start servers with graceful shutdown
    ctx, stop := signal.NotifyContext(context.Background(),
        syscall.SIGINT, syscall.SIGTERM)
    defer stop()

    // ... start HTTP server, ESL client
    // ... <-ctx.Done() for shutdown
}
```

**Why:** `main.go` is the composition root. It's the only place that knows about all packages. Business logic packages don't import each other's concrete types — they depend on interfaces. This makes the dependency graph a tree, not a web.

---

## Dependency Injection via Constructors

From Google Go Style Guide and Uber Go Style Guide: inject dependencies through constructor functions, not globals.

```go
// The constructor accepts dependencies as interfaces
type Engine struct {
    redis  RedisReader    // interface, not *redis.Client
    logger *zap.Logger
}

// RedisReader is defined WHERE IT'S USED (not in the redis package)
type RedisReader interface {
    Get(ctx context.Context, key string) (string, error)
    Exists(ctx context.Context, key string) (bool, error)
    EvalSHA(ctx context.Context, sha string, keys []string, args ...any) (any, error)
}

func NewEngine(redis RedisReader, logger *zap.Logger) *Engine {
    return &Engine{redis: redis, logger: logger}
}
```

**Why:** `Engine` doesn't know it's talking to Redis. It talks to a `RedisReader` interface. In tests, you pass a mock. In production, you pass the real Redis client. The `Engine` code is identical in both cases.

---

## Declaring Empty Slices

From Go Code Review Comments: "When declaring an empty slice, prefer `var t []string` over `t := []string{}`."

```go
// GOOD — nil slice (preferred)
var targets []Target

// BAD — non-nil but empty (unnecessary allocation)
targets := []Target{}
```

**Why:** A nil slice and an empty slice are functionally equivalent for `append`, `len`, `range`, and JSON marshaling (`null` vs `[]` is the only difference). Nil slices avoid an allocation. Use `[]T{}` only when you specifically need non-nil (e.g., a JSON response that must be `[]` not `null`).

---

## Import Organization

From Go Code Review Comments: standard library first, then external packages, separated by blank line.

```go
import (
    // Standard library
    "context"
    "encoding/json"
    "fmt"
    "net/http"
    "time"

    // External dependencies
    "github.com/go-chi/chi/v5"
    "github.com/redis/go-redis/v9"
    "go.uber.org/zap"

    // Internal packages
    "github.com/sageteck/dial-plan-router/internal/models"
    "github.com/sageteck/dial-plan-router/internal/routing"
)
```

Use `goimports` to auto-format. Three groups: stdlib, external, internal.

**Why:** Consistent import ordering makes diffs cleaner and makes it easy to spot new external dependencies in code review.
