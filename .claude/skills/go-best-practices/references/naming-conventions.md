# Naming Conventions

Sources: [Go Code Review Comments](https://go.dev/wiki/CodeReviewComments), [Google Go Style Guide](https://google.github.io/styleguide/go/best-practices.html), [Effective Go](https://go.dev/doc/effective_go)

---

## Variable Names

From Go Code Review Comments: "Variable names in Go should be short rather than long. This is especially true for local variables with limited scope."

**The rule: the further from its declaration a name is used, the more descriptive it must be.**

```go
// GOOD — short names for local scope
for i, v := range targets {
    if v.Enabled {
        available = append(available, v)
    }
}

// GOOD — single letter for method receivers, loop vars, readers
func (c *Client) Get(ctx context.Context, key string) (string, error)
func (e *Engine) Route(r *http.Request) (*Target, error)

// GOOD — descriptive for package-level or long-lived variables
var defaultTimeout = 30 * time.Second
var maxRetryAttempts = 5

// BAD — overly verbose local names
for targetIndex, currentTarget := range availableTargets {
    if currentTarget.IsEnabled {
        availableTargetsList = append(availableTargetsList, currentTarget)
    }
}
```

**Why:** Go code is meant to be concise. Short names reduce line length and visual noise. When the scope is small (a for loop, a 5-line function), the context is obvious and long names add nothing.

---

## MixedCaps

From Go Code Review Comments: "Go source code uses MixedCaps or mixedCaps rather than underscores when writing multi-word names."

```go
// GOOD
var maxLength int          // unexported
var MaxLength int          // exported
func parseConfig() {}      // unexported
func ParseConfig() {}      // exported
const defaultPort = 8090

// BAD — never use underscores or ALL_CAPS for Go identifiers
var max_length int
var MAX_LENGTH int
func parse_config() {}
const DEFAULT_PORT = 8090
```

**Why:** This is enforced by the Go community and tools like `golint`. It's not a preference — it's the language convention. `ALL_CAPS` is reserved for generated code or external constants.

---

## Initialisms

From Go Code Review Comments: "Words in names that are initialisms or acronyms have consistent case. 'URL' should appear as 'URL' or 'url', never as 'Url'."

```go
// GOOD
var serverHTTP *http.Server
var xmlHTTPRequest string
var appID string
type HTMLParser struct{}
func ServeHTTP(w http.ResponseWriter, r *http.Request)

// BAD
var serverHttp *http.Server
var xmlHttpRequest string
var appId string
type HtmlParser struct{}
func ServeHttp(w http.ResponseWriter, r *http.Request)
```

Common initialisms: `API`, `ASCII`, `CPU`, `CSS`, `DNS`, `EOF`, `GUID`, `HTML`, `HTTP`, `HTTPS`, `ID`, `IP`, `JSON`, `LHS`, `QPS`, `RAM`, `RHS`, `RPC`, `SLA`, `SMTP`, `SQL`, `SSH`, `TCP`, `TLS`, `TTL`, `UDP`, `UI`, `UID`, `UUID`, `URI`, `URL`, `UTF8`, `VM`, `XML`, `XMPP`, `XSRF`, `XSS`.

**Why:** Consistency. `appID` is clearly an identifier. `appId` looks like a proper name "Id". The Go community settled on full-caps initialisms and it's enforced by `golint`.

---

## Receiver Names

From Go Code Review Comments: "The name of a method's receiver should be a reflection of its identity; often a one or two letter abbreviation of its type suffices."

```go
// GOOD — 1-2 letter abbreviation of type
func (c *Client) Connect() error
func (e *Engine) Route(call *Call) (*Target, error)
func (b *Builder) Build() string
func (p *Producer) Produce(topic string, msg []byte) error

// BAD — generic OOP names
func (this *Client) Connect() error
func (self *Engine) Route(call *Call) (*Target, error)
func (me *Builder) Build() string

// BAD — inconsistent across methods of the same type
func (c *Client) Connect() error
func (cl *Client) Disconnect() error   // cl vs c — pick one
func (client *Client) Send() error     // too long
```

**Be consistent.** If `Client`'s receiver is `c` in one method, it must be `c` in ALL methods.

**Why:** In Go, the receiver is just another parameter — it has no special semantic meaning like `this` in Java or `self` in Python. Treating it as special misleads readers into thinking Go has OOP inheritance semantics. Short names also reduce line length in method chains.

---

## Package Names

From Go Code Review Comments and Effective Go:

```go
// GOOD — short, lowercase, no underscores, descriptive
package redis
package kafka
package routing
package dialplan
package config

// BAD — generic, stuttering, underscored
package util
package common
package helpers
package my_package
package routingEngine  // no MixedCaps in package names
```

**Don't stutter.** The package name is part of every qualified reference:

```go
// BAD — chubby.ChubbyFile, http.HTTPClient
package chubby
type ChubbyFile struct{}

// GOOD — chubby.File, http.Client
package chubby
type File struct{}
```

**Avoid generic names.** From Go Code Review Comments: "Avoid meaningless package names like util, common, misc, api, types, and interfaces."

```go
// BAD — what does "util" do?
import "myapp/util"
util.FormatTime(t)

// GOOD — clear purpose
import "myapp/timeformat"
timeformat.RFC3339(t)
```

**Why:** Package names appear in every call site. `redis.Client` is clear. `util.Client` is not. The package name provides context that makes the code self-documenting.

---

## Interface Names

From Effective Go: "By convention, one-method interfaces are named by the method name plus an -er suffix."

```go
// GOOD — standard Go convention
type Reader interface {
    Read(p []byte) (n int, err error)
}

type Writer interface {
    Write(p []byte) (n int, err error)
}

type Stringer interface {
    String() string
}

// GOOD — for our project
type TargetChecker interface {
    CheckAvailability(ctx context.Context, t *Target) (bool, error)
}

type RouteSelector interface {
    Select(ctx context.Context, routes []Route) (*Route, error)
}
```

**Don't prefix interfaces with "I"** (this is a Java/C# convention, not Go):

```go
// BAD — Java-style
type IRouter interface{}
type IProducer interface{}

// GOOD — Go-style
type Router interface{}
type Producer interface{}
```

---

## Function and Method Names

From Google Go Style Guide: "Avoid repetition in function/method names."

```go
// BAD — type name repeated
func (c *Config) GetConfigValue(key string) string
func ParseYAMLConfig(input string) (*Config, error)

// GOOD — package and receiver provide context
func (c *Config) Value(key string) string          // c.Value("key")
func Parse(input string) (*Config, error)           // config.Parse(input)
```

From Google Go Style Guide: "Use noun-like names for getters, verb-like for actions. Don't use Get prefix."

```go
// GOOD — noun for value retrieval (no "Get")
func (c *Campaign) ID() string
func (t *Target) Destination() string

// GOOD — verb for actions
func (e *Engine) Route(ctx context.Context, call *Call) (*Target, error)
func (p *Producer) Produce(topic string, msg []byte) error

// BAD — unnecessary Get prefix
func (c *Campaign) GetID() string
func (t *Target) GetDestination() string
```

**Why:** `campaign.ID()` reads naturally. `campaign.GetID()` adds noise without clarity. The receiver type already tells you this is a Campaign — `Get` adds nothing. This is a deliberate departure from Java/C# conventions.

---

## Doc Comments

From Go Code Review Comments: "All top-level, exported names should have doc comments. Comments should begin with the name of the thing being described and end in a period."

```go
// GOOD
// Engine evaluates routing rules and selects a target for inbound calls.
type Engine struct{}

// Route applies availability checks and routing strategy to select
// the best target for the given call. It returns ErrNoTargets if no
// targets are available after filtering.
func (e *Engine) Route(ctx context.Context, call *Call) (*Target, error)

// BAD — doesn't start with the name
// This struct handles routing.
type Engine struct{}

// BAD — no period, doesn't start with function name
// routes a call to a target
func (e *Engine) Route(ctx context.Context, call *Call) (*Target, error)
```

**Why:** Go's `godoc` tool extracts these comments to build documentation. Starting with the name makes the docs scannable. The period makes them render as proper sentences.
