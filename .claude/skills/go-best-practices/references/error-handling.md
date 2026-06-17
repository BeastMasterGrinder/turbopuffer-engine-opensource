# Error Handling Patterns

Sources: [Go Code Review Comments](https://go.dev/wiki/CodeReviewComments), [Google Go Style Guide](https://google.github.io/styleguide/go/best-practices.html), [Go 1.13 Errors Blog](https://go.dev/blog/go1.13-errors)

---

## Rule 1: Never Ignore Errors

From Go Code Review Comments: "Do not discard errors using `_` variables. If a function returns an error, check it to make sure the function succeeded."

```go
// BAD — error silently ignored, program continues in invalid state
val, _ := strconv.Atoi(input)

// GOOD — handle or return
val, err := strconv.Atoi(input)
if err != nil {
    return fmt.Errorf("parsing input %q: %w", input, err)
}
```

**Why:** Ignoring errors means your program may continue with invalid data. A silent failure here causes a crash somewhere else later — much harder to debug.

---

## Rule 2: Indent Error Flow, Keep Happy Path Left-Aligned

From Go Code Review Comments: "Try to keep the normal code path at a minimal indentation, and indent the error handling, dealing with it first."

```go
// BAD — happy path is indented
if err != nil {
    // error handling
} else {
    // normal code that keeps going
    // and going
    // and going
}

// GOOD — handle error, return early, happy path stays left
if err != nil {
    return err
}
// normal code at top level
// clear and easy to scan
```

**Why:** When scanning a function, you read the left edge. If the happy path is always at the left, you can quickly understand what the function does. Error handling is the branch, not the trunk.

---

## Rule 3: Wrap Errors with Context Using %w

From Google Go Style Guide: "Use `%w` for internal error chaining." From Go 1.13 blog: "An easy way to create wrapped errors is to call `fmt.Errorf` and apply the `%w` verb."

```go
// BAD — context lost
func readConfig(path string) (*Config, error) {
    data, err := os.ReadFile(path)
    if err != nil {
        return nil, err  // caller sees: "open /etc/config.yaml: no such file"
                         // but doesn't know WHO tried to open it or WHY
    }
    // ...
}

// GOOD — adds context, preserves original error for inspection
func readConfig(path string) (*Config, error) {
    data, err := os.ReadFile(path)
    if err != nil {
        return nil, fmt.Errorf("reading config file: %w", err)
        // caller sees: "reading config file: open /etc/config.yaml: no such file"
    }
    // ...
}
```

**Place `%w` at the end** of the format string (Google Style Guide). This ensures error chains print newest-to-oldest:

```go
// GOOD
return fmt.Errorf("connecting to redis: %w", err)

// BAD
return fmt.Errorf("%w: connecting to redis", err)
```

**Why:** Error chains are like stack traces for business logic. Each layer adds "what I was doing when this failed." Without wrapping, you get a raw OS error with no application context.

---

## Rule 4: Error Strings Are Lowercase, No Punctuation

From Go Code Review Comments: "Error strings should not be capitalized (unless beginning with proper nouns or acronyms) or end with punctuation, since they are usually printed following other context."

```go
// BAD
fmt.Errorf("Failed to connect to Redis.")

// GOOD
fmt.Errorf("connecting to redis")

// GOOD — proper noun is OK to capitalize
fmt.Errorf("connecting to Redis")
```

**Why:** Errors are composed. `log.Printf("reading config: %v", err)` would print `"reading config: Failed to connect to Redis."` — capital in the middle, double punctuation. Lowercase errors compose cleanly.

---

## Rule 5: Sentinel Errors for Known Conditions

From Google Go Style Guide: "Use sentinel values so callers can distinguish error conditions without string matching."

```go
// Define sentinel errors at package level
var (
    ErrNotFound     = errors.New("not found")
    ErrAtCapacity   = errors.New("at capacity")
    ErrDuplicateCall = errors.New("duplicate caller")
)

// Return them
func findTarget(id string) (*Target, error) {
    data, err := redis.Get(ctx, "target:"+id)
    if errors.Is(err, redis.Nil) {
        return nil, ErrNotFound
    }
    if err != nil {
        return nil, fmt.Errorf("fetching target %s: %w", id, err)
    }
    // ...
}

// Check them with errors.Is (handles wrapped errors)
target, err := findTarget(id)
if errors.Is(err, ErrNotFound) {
    // target doesn't exist — return hangup XML
}
```

**Why:** String matching (`strings.Contains(err.Error(), "not found")`) is fragile. Sentinel errors give callers a stable, typed contract. `errors.Is` walks the entire wrap chain, so it works even if the error was wrapped multiple times.

---

## Rule 6: Custom Error Types for Rich Error Data

From Google Go Style Guide: "Give error values structure so callers can interrogate errors programmatically."

```go
// Custom error type with structured data
type CapError struct {
    TargetID string
    Current  int
    Cap      int
}

func (e *CapError) Error() string {
    return fmt.Sprintf("target %s at capacity (%d/%d)", e.TargetID, e.Current, e.Cap)
}

// Return it
func checkConcurrentCap(targetID string, cap int) error {
    current, err := redis.Get(ctx, "cap:concurrent:"+targetID)
    if err != nil {
        return fmt.Errorf("checking cap for %s: %w", targetID, err)
    }
    if current >= cap {
        return &CapError{TargetID: targetID, Current: current, Cap: cap}
    }
    return nil
}

// Inspect it with errors.As
var capErr *CapError
if errors.As(err, &capErr) {
    log.Info("target at capacity",
        zap.String("target_id", capErr.TargetID),
        zap.Int("current", capErr.Current),
        zap.Int("cap", capErr.Cap),
    )
}
```

**Why:** `errors.Is` checks identity (is this the exact error?). `errors.As` checks type (is this a certain kind of error?) and extracts the data. Use custom types when callers need more than just "which error" — they need "what happened."

---

## Rule 7: Don't Log Errors You Return

From Google Go Style Guide: "Avoid duplication... let the caller handle it."

```go
// BAD — double logging
func loadCampaign(id string) (*Campaign, error) {
    data, err := redis.Get(ctx, "campaign:"+id)
    if err != nil {
        log.Error("failed to load campaign", zap.Error(err))  // logged here
        return nil, fmt.Errorf("loading campaign: %w", err)    // AND returned
    }
    // ...
}

// GOOD — return only, caller decides
func loadCampaign(id string) (*Campaign, error) {
    data, err := redis.Get(ctx, "campaign:"+id)
    if err != nil {
        return nil, fmt.Errorf("loading campaign %s: %w", id, err)
    }
    // ...
}
```

**Why:** If every layer logs AND returns, you get the same error logged 5 times at different levels. Log at the top of the call chain where you have full context and can decide the appropriate action.

---

## Rule 8: Don't Panic for Normal Errors

From Go Code Review Comments: "Don't use panic for normal error handling. Use error and multiple return values."

```go
// BAD — panic for a recoverable situation
func getTarget(id string) *Target {
    t, err := lookupTarget(id)
    if err != nil {
        panic(err)  // crashes the whole server
    }
    return t
}

// GOOD — return the error
func getTarget(id string) (*Target, error) {
    t, err := lookupTarget(id)
    if err != nil {
        return nil, fmt.Errorf("getting target %s: %w", id, err)
    }
    return t, nil
}
```

**Why:** Panics crash the goroutine (and the entire program unless recovered). In a server handling live calls, a panic in the routing handler drops the call AND potentially all other in-flight calls. Return errors; let the caller decide what to do.

**Exception:** Panics are acceptable in `init()` or startup code where the program genuinely cannot function (e.g., missing required config). Even then, prefer `log.Fatal`.
