# Testing Patterns

Sources: [Google Go Style Guide](https://google.github.io/styleguide/go/best-practices.html), [Uber Go Style Guide](https://github.com/uber-go/guide/blob/master/style.md), [Go Code Review Comments](https://go.dev/wiki/CodeReviewComments)

---

## Table-Driven Tests

From Uber Go Style Guide: structure tests as a slice of test cases with `t.Run` subtests.

```go
func TestIsWithinHours(t *testing.T) {
    tests := []struct {
        name     string
        timezone string
        hours    map[string]any
        now      time.Time
        want     bool
    }{
        {
            name:     "within business hours",
            timezone: "America/New_York",
            hours:    map[string]any{"monday": map[string]string{"open": "08:00", "close": "18:00"}},
            now:      time.Date(2026, 4, 6, 14, 30, 0, 0, mustLoadLocation("America/New_York")), // Monday 2:30 PM
            want:     true,
        },
        {
            name:     "outside business hours",
            timezone: "America/New_York",
            hours:    map[string]any{"monday": map[string]string{"open": "08:00", "close": "18:00"}},
            now:      time.Date(2026, 4, 6, 19, 0, 0, 0, mustLoadLocation("America/New_York")), // Monday 7 PM
            want:     false,
        },
        {
            name:     "closed day returns false",
            timezone: "America/New_York",
            hours:    map[string]any{"sunday": nil},
            now:      time.Date(2026, 4, 5, 12, 0, 0, 0, mustLoadLocation("America/New_York")), // Sunday noon
            want:     false,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            got := isWithinHours(tt.hours, tt.timezone, tt.now)
            if got != tt.want {
                t.Errorf("isWithinHours() = %v, want %v", got, tt.want)
            }
        })
    }
}
```

**Why table-driven:** Adding a new test case is one struct literal. The test logic is written once. `t.Run` gives each case a name that shows up in output: `--- FAIL: TestIsWithinHours/closed_day_returns_false`. You can run a single case with `go test -run TestIsWithinHours/closed_day`.

---

## Useful Failure Messages

From Go Code Review Comments: "Tests should fail with helpful messages saying what was wrong, with what inputs, what was actually got, and what was expected."

```go
// BAD — useless failure message
if got != want {
    t.Error("test failed")
}

// BAD — only shows values, no context
if got != want {
    t.Errorf("got %v, want %v", got, want)
}

// GOOD — shows function, inputs, got, want
if got != want {
    t.Errorf("selectWeighted(targets, seed=42) = %q, want %q", got, want)
}

// GOOD — for complex structs, use cmp.Diff
if diff := cmp.Diff(want, got); diff != "" {
    t.Errorf("Route() mismatch (-want +got):\n%s", diff)
}
```

**Why:** When a test fails in CI at 2 AM, the error message is all you have. `"test failed"` tells you nothing. `"selectWeighted(targets, seed=42) = target_11, want target_10"` tells you exactly what to investigate.

---

## Test Helpers with t.Helper()

From Google Go Style Guide: use `t.Helper()` in helper functions so failures report the caller's line, not the helper's line.

```go
// Helper function marks itself
func mustLoadLocation(name string) *time.Location {
    // This is a test-only helper, so panic is acceptable
    loc, err := time.LoadLocation(name)
    if err != nil {
        panic(fmt.Sprintf("loading timezone %q: %v", name, err))
    }
    return loc
}

// Helper that uses t.Helper()
func assertNoError(t *testing.T, err error) {
    t.Helper() // failure will report caller's line, not this line
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
}

// Setup helper
func setupTestRedis(t *testing.T) *redis.Client {
    t.Helper()
    client := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 15})
    t.Cleanup(func() {
        client.FlushDB(context.Background())
        client.Close()
    })
    return client
}
```

**Why:** Without `t.Helper()`, a failure in `assertNoError` reports the line inside `assertNoError` — not the test that called it. With `t.Helper()`, the failure points to the actual test case, which is where you need to look.

---

## Never Call t.Fatal from a Goroutine

From Google Go Style Guide: "It is incorrect to call t.FailNow, t.Fatal, etc. from any goroutine but the one running the Test function."

```go
// BAD — t.Fatal from a goroutine causes undefined behavior
func TestConcurrent(t *testing.T) {
    go func() {
        if err := doWork(); err != nil {
            t.Fatalf("work failed: %v", err)  // UNDEFINED BEHAVIOR
        }
    }()
}

// GOOD — use t.Error (non-fatal) from goroutines
func TestConcurrent(t *testing.T) {
    var wg sync.WaitGroup
    wg.Add(1)
    go func() {
        defer wg.Done()
        if err := doWork(); err != nil {
            t.Errorf("work failed: %v", err)  // safe from goroutine
        }
    }()
    wg.Wait()
}
```

**Why:** `t.Fatal` calls `runtime.Goexit()` which only terminates the current goroutine, not the test goroutine. This leaves the test in an inconsistent state and can cause panics or hangs.

---

## Mocking with Interfaces

From Go Code Review Comments: "Go interfaces generally belong in the package that uses values of the interface type." From Google Go Style Guide: "Do not define interfaces on the implementor side of an API 'for mocking'."

```go
// Define the interface WHERE IT'S CONSUMED (in routing package, not redis package)
// internal/routing/engine.go

type RedisReader interface {
    Get(ctx context.Context, key string) (string, error)
    Exists(ctx context.Context, keys ...string) (int64, error)
    EvalSHA(ctx context.Context, sha string, keys []string, args ...any) (any, error)
}

type Engine struct {
    redis RedisReader
}

// In tests, provide a mock that satisfies the interface
// internal/routing/engine_test.go

type mockRedis struct {
    data    map[string]string
    evalFn  func(sha string, keys []string, args ...any) (any, error)
}

func (m *mockRedis) Get(_ context.Context, key string) (string, error) {
    v, ok := m.data[key]
    if !ok {
        return "", redis.Nil
    }
    return v, nil
}

func (m *mockRedis) Exists(_ context.Context, keys ...string) (int64, error) {
    var count int64
    for _, k := range keys {
        if _, ok := m.data[k]; ok {
            count++
        }
    }
    return count, nil
}

func (m *mockRedis) EvalSHA(_ context.Context, sha string, keys []string, args ...any) (any, error) {
    if m.evalFn != nil {
        return m.evalFn(sha, keys, args...)
    }
    return int64(1), nil // default: cap check passes
}

func TestRouteCall(t *testing.T) {
    mock := &mockRedis{
        data: map[string]string{
            "tracking:+18005550100": "42",
            "campaign:42":          `{"routing_type":"priority","ivr_enabled":false}`,
            "campaign:42:targets":  `[...]`,
            "health:10":            "live",
        },
    }

    engine := NewEngine(mock, zap.NewNop())
    target, err := engine.Route(context.Background(), &Call{
        DNIS: "+18005550100",
        ANI:  "+12025551234",
    })

    if err != nil {
        t.Fatalf("Route() error: %v", err)
    }
    if target.ID != "10" {
        t.Errorf("Route() selected target %s, want 10", target.ID)
    }
}
```

**Why:** The interface is tiny (3 methods — only what Engine needs). The mock is simple (a map). No mock framework needed. The real Redis client satisfies this interface automatically because Go interfaces are implicit. You never wrote `type Client implements RedisReader` — it just works.

---

## t.Cleanup for Test Resource Management

```go
func TestWithRedis(t *testing.T) {
    client := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 15})

    // t.Cleanup runs AFTER the test (and all subtests) finish
    // Like defer, but scoped to the test — not the function
    t.Cleanup(func() {
        client.FlushDB(context.Background())
        client.Close()
    })

    // ... test code using client
}
```

**Why:** `t.Cleanup` is safer than `defer` in tests because it runs after subtests complete. If your test spawns subtests with `t.Run`, a `defer` in the parent runs before the subtests finish — `t.Cleanup` waits for everything.

---

## Test File Naming

```
internal/routing/
├── engine.go           # production code
├── engine_test.go      # tests for engine.go
├── strategy.go         # production code
├── strategy_test.go    # tests for strategy.go
└── availability.go     # production code
```

Test files live next to the code they test. Same package, `_test.go` suffix. Go's build system automatically excludes `_test.go` files from production builds.
