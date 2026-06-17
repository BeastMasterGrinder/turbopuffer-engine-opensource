---
name: go-best-practices
description: Go best practices and idiomatic patterns for writing production Go code. Use this skill whenever writing, reviewing, or modifying Go code (.go files). Also trigger when the user asks about Go conventions, style, error handling, concurrency, testing, or project structure. Even if you think you know Go well, consult this skill — it contains verified rules from Google's Go Style Guide, Uber's Go Style Guide, and the official Go Code Review Comments wiki.
---

# Go Best Practices

This skill contains Go best practices verified from three authoritative sources:
- [Go Code Review Comments](https://go.dev/wiki/CodeReviewComments) — Official Go project wiki
- [Google Go Style Guide](https://google.github.io/styleguide/go/best-practices.html) — Google's production Go standards
- [Uber Go Style Guide](https://github.com/uber-go/guide/blob/master/style.md) — Uber's production Go guide

## Core Principles (Google Go Style Guide)

Readable Go code prioritizes these attributes, in order:

1. **Clarity** — The code's purpose and rationale is clear to the reader
2. **Simplicity** — The code accomplishes its goal in the simplest way
3. **Concision** — High signal-to-noise ratio
4. **Maintainability** — Code is easily maintained over time
5. **Consistency** — Consistent with the broader codebase and Go ecosystem

When these conflict, prefer the one higher on the list.

---

## Quick Reference Rules

These are the rules to keep in your head at all times when writing Go.

### Error Handling

- **Never ignore errors.** Do not discard errors with `_`. Handle it, return it, or in truly exceptional cases, panic.
- **Wrap errors with context** using `fmt.Errorf("doing X: %w", err)`. Place `%w` at the end.
- **Error strings are lowercase**, no punctuation. They get printed after other context: `fmt.Errorf("something bad")` not `fmt.Errorf("Something bad.")`.
- **Use `errors.Is` and `errors.As`** for inspection, never string matching.
- **Indent the error path**, keep the happy path at minimal indentation. Return early on error.
- **Don't log errors you return.** The caller decides whether to log.
- Read `references/error-handling.md` for patterns and code examples.

### Naming

- **Short names for local variables.** `c` not `lineCount`, `i` not `sliceIndex`. The further from declaration, the more descriptive.
- **MixedCaps always.** `maxLength` not `max_length` or `MAX_LENGTH`. Exported: `MaxLength`.
- **Initialisms stay capitalized.** `ServeHTTP`, `appID`, `xmlHTTPRequest` — never `ServeHttp` or `appId`.
- **Receiver names are 1-2 letters** of the type. `c` for `Client`, never `this`, `self`, `me`. Be consistent across methods.
- **Package names are singular, lowercase, no underscores.** Avoid generic names like `util`, `common`, `helpers`.
- **Don't stutter.** In package `http`, name it `Client` not `HTTPClient` (users write `http.Client`).
- Read `references/naming-conventions.md` for full rules.

### Interfaces

- **Accept interfaces, return structs.** Functions should accept interface parameters and return concrete types.
- **Define interfaces where they're consumed**, not where they're implemented.
- **Keep interfaces small.** One or two methods. `io.Reader` has one method and it's one of the most powerful interfaces in Go.
- **Don't define interfaces before you need them.** Design emerges from real usage, not speculation.
- **Don't define interfaces "for mocking."** Design the API so it can be tested using the real implementation's public API.

### Context

- **Always the first parameter**: `func DoThing(ctx context.Context, arg string) error`.
- **Never store in a struct.** Pass it explicitly to every function that needs it.
- **Don't create custom Context types.** Use the standard `context.Context`.
- Only use `context.Background()` at the top of the call chain (main, init, tests).

### Concurrency

- **Every goroutine must have a clear exit strategy.** Document when and why it exits.
- **Prefer synchronous functions.** Let callers add concurrency if they need it. Removing concurrency is much harder than adding it.
- **Channel size is zero (unbuffered) or one.** Anything larger needs serious justification (Uber guide).
- **Use `sync.WaitGroup`** to wait for goroutine completion.
- **Use `context.Context`** for cancellation signals.
- **Never start goroutines in `init()`.** It creates unpredictable startup and makes testing hard.
- Read `references/concurrency-patterns.md` for production patterns.

### Structs

- **Design for useful zero values.** A zero-value `sync.Mutex` is usable. A zero-value `bytes.Buffer` is a ready-to-use buffer. Design your structs the same way.
- **Composition over inheritance.** Embed types to compose behavior, use interfaces for polymorphism.
- **Avoid embedding in public structs** (Uber guide). Embedded types expose their methods as your API. Use named fields.
- **Use functional options** for constructors with many optional parameters.

### Testing

- **Table-driven tests** for testing multiple cases. Use `t.Run` with descriptive names.
- **Fail with helpful messages**: `t.Errorf("Foo(%q) = %d; want %d", tt.in, got, tt.want)`.
- **Never call `t.Fatal` from a goroutine.** Use `t.Error` instead.
- **Use `t.Helper()`** in test helper functions so failures report the caller's line.
- Read `references/testing-patterns.md` for examples.

### Project Structure

- **`cmd/`** for executable entry points.
- **`internal/`** for packages that must not be imported by external modules. Go enforces this.
- **One package = one responsibility.** A package named `util` is a code smell.
- Read `references/project-structure.md` for the full layout.

### Graceful Shutdown

- **Use `signal.NotifyContext`** (Go 1.16+) to tie OS signals to context cancellation.
- **Order your cleanup**: stop accepting connections → drain in-flight work → close resources.
- **Always set a shutdown timeout.** Don't wait forever for connections to drain.
- Read `references/graceful-shutdown.md` for the production pattern.

---

## Reference Files

Read these for detailed code examples when working on specific areas:

| File | When to Read |
|---|---|
| `references/error-handling.md` | Writing error returns, wrapping, sentinel errors, custom error types |
| `references/naming-conventions.md` | Naming variables, functions, receivers, packages, interfaces |
| `references/concurrency-patterns.md` | Spawning goroutines, worker pools, context cancellation, channels |
| `references/project-structure.md` | Package layout, dependency injection, cmd/internal conventions |
| `references/graceful-shutdown.md` | Signal handling, HTTP server shutdown, ordered cleanup |
| `references/testing-patterns.md` | Table-driven tests, test helpers, mocking with interfaces |

---

## Sources

Every rule in this skill and its reference files comes from one of these verified sources:

- [Go Code Review Comments](https://go.dev/wiki/CodeReviewComments) — Official Go project wiki
- [Google Go Style Guide](https://google.github.io/styleguide/go/best-practices.html) — Best Practices section
- [Google Go Style Decisions](https://google.github.io/styleguide/go/decisions.html)
- [Uber Go Style Guide](https://github.com/uber-go/guide/blob/master/style.md)
- [Effective Go](https://go.dev/doc/effective_go)
- [Go Language Specification](https://go.dev/ref/spec)
