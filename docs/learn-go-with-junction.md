# Learning Go through this codebase

A reading order for someone who knows Java and has never written Go. Each step is a file or two, plus the Go ideas it introduces. Read the code with the Java comparison in mind.

## 0. Run it first

```sh
go run ./cmd/junction demo     # compile + run in one step
go test ./...                  # every package's tests
go vet ./...                   # the built-in linter
gofmt -w .                     # the one true formatting, no debates
```

A **module** (`go.mod`) is like a Maven project. A **package** is one folder, and every `.go` file in the folder shares it. Anything under `internal/` can only be imported from inside this module, which works like package-private at the module level.

## 1. `internal/jsonrpc/jsonrpc.go`: structs, methods, errors

- `type Message struct {…}` with `json:"id,omitempty"` tags. Struct tags replace Jackson annotations.
- `func (m *Message) IsRequest() bool` is a **method**: a function with a receiver. There are no classes. Capitalized names are exported (public); lowercase ones are package-private.
- `json.RawMessage` keeps bytes undecoded. That's how the gateway forwards fields it doesn't understand.
- Errors are values. `*Error` implements the built-in `error` interface just by having an `Error() string` method. There's no `implements` keyword: interfaces are satisfied implicitly.

## 2. `internal/config/config.go`: zero values, embedding files

- Fields you don't set get **zero values** (`""`, `0`, `nil`). `applyDefaults` fills them in.
- `//go:embed example.yaml` compiles a file into the binary (see the bottom of the file). The dashboard HTML ships the same way.
- `fmt.Errorf("config: %w", err)` wraps an error, and `errors.Is` / `errors.As` unwrap it, roughly like exception causes.

## 3. `internal/upstream/stdio.go`: goroutines and channels

This is the most "Go" file in the project.

- `go t.readLoop(stdout)` starts a **goroutine**, a very cheap thread. Junction runs several per server.
- `pending map[string]chan *jsonrpc.Message`: each in-flight request waits on its own **channel**. The read loop finds the channel by id and sends the response into it.
- `select { case m := <-ch: … case <-ctx.Done(): … }` waits on whichever happens first: the response or a timeout or cancellation.
- `sync.Mutex` guards the map, just like `synchronized`. Go's motto: *share memory by communicating*, but use a mutex when that's simpler.

## 4. `internal/upstream/client.go`: interfaces and context

- `type transport interface { roundTrip(…); notify(…); close() }` has two implementations (stdio, HTTP). `Client` doesn't care which one it has.
- `context.Context` is passed as the first argument everywhere. It carries deadlines and cancellation. `context.WithTimeout` plus `defer cancel()` is the idiom.
- `defer` runs at function exit, like `finally`.
- The era-detection logic (`connectLocked`) is the spec's "dual-era client" algorithm. Compare it with the compatibility matrix in the spec.

## 5. `internal/gateway/`: the core

- `gateway.go`: `Handle` → `dispatch` → one handler per MCP method. A `switch` on strings needs no `break`.
- `catalog.go`: `sync.WaitGroup` fans out to all servers in parallel and waits for all of them. An atomic flag marks the catalog stale.
- `server.go`: `ServeHTTP(w, r)` makes `Gateway` an `http.Handler`, much like a Java servlet's `service()` method.
- `gateway_test.go`: table-free, plain tests. `httptest.NewServer` runs a real HTTP server inside the test. `TestMain` lets the test binary pretend to be the demo MCP servers.

## 6. `cmd/junction/main.go`: the program

`package main` + `func main()`. Subcommands use the standard `flag` package. `signal.NotifyContext` turns Ctrl+C into a cancelled context, and the server shuts down gracefully.

## Exercises (good first contributions)

1. Add an `ip_address` redaction rule (with a test in `redact_test.go`).
2. Add a `max_calls_per_minute` per tool to `policy` (hint: a `map[string]*rate` guarded by a mutex).
3. Show each server's p95 latency on the Servers tab (the data is in `recorder.Stats`).
4. Add `--json` to `junction tools` for machine-readable output.
