# Concurrency Patterns

Sources: [Go Code Review Comments](https://go.dev/wiki/CodeReviewComments), [Uber Go Style Guide](https://github.com/uber-go/guide/blob/master/style.md), [Effective Go](https://go.dev/doc/effective_go)

---

## Rule 1: Every Goroutine Must Have a Clear Exit Strategy

From Go Code Review Comments: "When you spawn goroutines, make it clear when — or whether — they exit."

From Uber Go Style Guide: "Goroutines are lightweight, but they're not free: at minimum, they cost memory for their stack and CPU to be scheduled."

```go
// BAD — goroutine leaks forever if channel is never read
func startWorker() {
    ch := make(chan Result)
    go func() {
        result := doExpensiveWork()
        ch <- result  // blocks forever if nobody reads ch
    }()
    // ch is never read — goroutine leaks
}

// GOOD — context cancellation provides exit strategy
func startWorker(ctx context.Context) <-chan Result {
    ch := make(chan Result, 1)
    go func() {
        defer close(ch)
        select {
        case ch <- doExpensiveWork():
        case <-ctx.Done():
            return  // exit when context cancelled
        }
    }()
    return ch
}
```

**Why:** A leaked goroutine holds its entire stack in memory (2-8KB minimum), keeps any objects it references from being garbage collected, and wastes CPU time when the scheduler tries to run it. In a high-throughput service handling thousands of calls, leaked goroutines accumulate and eventually OOM the process.

---

## Rule 2: Prefer Synchronous Functions

From Go Code Review Comments: "Prefer synchronous functions — functions which return their results directly — over asynchronous ones."

```go
// BAD — forces concurrency on the caller
func LookupCampaign(id string) <-chan *Campaign {
    ch := make(chan *Campaign, 1)
    go func() {
        campaign := fetchFromRedis(id)
        ch <- campaign
    }()
    return ch
}

// GOOD — caller decides if concurrency is needed
func LookupCampaign(ctx context.Context, id string) (*Campaign, error) {
    return fetchFromRedis(ctx, id)
}

// Caller adds concurrency if they want it:
go func() {
    campaign, err := LookupCampaign(ctx, id)
    // ...
}()
```

**Why:** Synchronous functions keep goroutines localized within a call, making it easy to reason about lifetimes. Removing unwanted concurrency from an async API is difficult or impossible. Adding concurrency to a sync API is trivial.

---

## Rule 3: Use sync.WaitGroup to Wait for Goroutines

From Uber Go Style Guide:

```go
// Pattern: launch N workers, wait for all to finish
func processTargets(ctx context.Context, targets []Target) error {
    var wg sync.WaitGroup

    for _, t := range targets {
        wg.Add(1)
        go func() {
            defer wg.Done()
            checkAvailability(ctx, t)
        }()
    }

    wg.Wait()  // blocks until all goroutines call Done()
    return nil
}
```

For a single goroutine, a done channel is simpler (Uber guide):

```go
done := make(chan struct{})
go func() {
    defer close(done)
    // ... work
}()

// Wait for goroutine to finish:
<-done
```

**Why:** `sync.WaitGroup` is the standard coordination primitive for fan-out/fan-in patterns. It's safe for concurrent use and clearly communicates "I'm waiting for N things to finish."

---

## Rule 4: Channel Size is Zero or One

From Uber Go Style Guide: "Channels should have a size of zero (unbuffered) or one. Any larger size requires high level of scrutiny."

```go
// GOOD — unbuffered (synchronous handoff)
ch := make(chan Event)

// GOOD — buffered with size 1 (non-blocking single send)
ch := make(chan Event, 1)

// BAD — arbitrary buffer hides synchronization problems
ch := make(chan Event, 64)
```

**Why:** Large buffers mask backpressure. If a producer sends faster than a consumer reads, a buffer of 64 just delays the problem by 64 messages. When the buffer fills, you still block — but now you have 64 messages of latency to debug. Unbuffered channels make synchronization explicit.

**Exception:** Worker pools with a known, fixed number of workers can use a channel sized to the worker count.

---

## Rule 5: Use Context for Cancellation

```go
// Pattern: pass context through the call chain
func (e *Engine) Route(ctx context.Context, call *Call) (*Target, error) {
    // Check cancellation before expensive work
    select {
    case <-ctx.Done():
        return nil, ctx.Err()
    default:
    }

    campaign, err := e.redis.GetCampaign(ctx, call.CampaignID)
    if err != nil {
        return nil, fmt.Errorf("loading campaign: %w", err)
    }

    targets, err := e.redis.GetTargets(ctx, campaign.ID)
    if err != nil {
        return nil, fmt.Errorf("loading targets: %w", err)
    }

    return e.selectTarget(ctx, campaign, targets, call)
}
```

**Why:** Context cancellation propagates through the entire call tree. When the HTTP request is cancelled (client disconnects, timeout), every downstream operation gets notified simultaneously. Without context, you'd need to thread cancellation channels manually through every function.

---

## Rule 6: Never Start Goroutines in init()

From Uber Go Style Guide: "Launching goroutines during package initialization creates unpredictable startup behavior and makes testing difficult."

```go
// BAD — goroutine in init, runs before main(), can't be controlled
func init() {
    go backgroundCleanup()  // when does this stop? how do you test this?
}

// GOOD — explicit start controlled by caller
type Service struct {
    cancel context.CancelFunc
}

func NewService() *Service {
    return &Service{}
}

func (s *Service) Start(ctx context.Context) {
    ctx, s.cancel = context.WithCancel(ctx)
    go s.backgroundCleanup(ctx)
}

func (s *Service) Stop() {
    s.cancel()
}
```

**Why:** `init()` runs at import time, before `main()`. You can't pass configuration, can't control ordering, can't cancel the goroutine, and can't write meaningful tests. Explicit start functions give callers control.

---

## Rule 7: Fire-and-Forget Pattern (for Kafka Produces)

This is the pattern we use for non-blocking Kafka produces in the routing hot path:

```go
// Pattern: fire-and-forget with error logging
func (h *Handler) handleDialplan(w http.ResponseWriter, r *http.Request) {
    // ... routing logic, build XML ...

    // Return response immediately — don't wait for Kafka
    w.Header().Set("Content-Type", "text/xml")
    w.Write(xmlBytes)

    // Fire-and-forget: runs after response is sent
    go func() {
        if err := h.producer.Produce(ctx, "routing.decisions", msg); err != nil {
            h.logger.Error("failed to produce routing decision",
                zap.Error(err),
                zap.String("call_id", callID),
            )
            // Don't crash — the call is already routed successfully
        }
    }()
}
```

**Why:** The Kafka produce must never block the dialplan response. FreeSWITCH has a 200ms timeout. If Kafka is slow or down, we still return the routing XML. The Kafka message is best-effort — losing a routing decision event is acceptable; dropping a live call is not.

---

## Rule 8: Protect Shared State

From Go Code Review Comments: "Package-level variables can cause data races."

```go
// BAD — data race if called from multiple goroutines
var count int
func increment() {
    count++  // not atomic — race condition
}

// GOOD — use sync.Mutex
type Counter struct {
    mu    sync.Mutex
    count int
}

func (c *Counter) Increment() {
    c.mu.Lock()
    defer c.mu.Unlock()
    c.count++
}

// GOOD — use atomic for simple counters
var count atomic.Int64
func increment() {
    count.Add(1)
}
```

**Why:** Go's race detector (`go test -race`) will catch these, but only if you test the concurrent path. Prevent races by design: either use mutexes, atomics, or confine mutable state to a single goroutine that processes requests via channels.

---

## Rule 9: Avoid Mutable Globals

From Uber Go Style Guide:

```go
// BAD — mutable global, makes testing hard
var redisClient *redis.Client

func init() {
    redisClient = redis.NewClient(...)
}

func GetCampaign(id string) (*Campaign, error) {
    return redisClient.Get(...)  // uses global — can't mock in tests
}

// GOOD — inject dependencies
type CampaignStore struct {
    redis *redis.Client
}

func NewCampaignStore(client *redis.Client) *CampaignStore {
    return &CampaignStore{redis: client}
}

func (s *CampaignStore) Get(ctx context.Context, id string) (*Campaign, error) {
    return s.redis.Get(ctx, ...)
}
```

**Why:** Mutable globals create hidden coupling between packages, make tests non-deterministic (tests share state), and prevent running tests in parallel. Dependency injection through constructors makes dependencies explicit and testable.
