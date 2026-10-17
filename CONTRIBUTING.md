# Contributing to Junction

Thanks for helping. Bug reports, docs fixes and features are all welcome.

## Getting started

```sh
git clone https://github.com/harshmangalamv/the-junction
cd the-junction
go test -race ./...      # unit + end-to-end tests
go run ./cmd/junction demo
```

You need Go 1.24 or newer. There are no other dependencies: the tests start their own demo MCP servers.

New to Go or to this codebase? [docs/learn-go-with-junction.md](docs/learn-go-with-junction.md) walks through it file by file and ends with a few good first exercises.

## Making a change

1. For anything bigger than a small fix, open an issue first so we can agree on the approach.
2. Keep pull requests focused: one change per PR.
3. Add or update tests. Protocol behaviour belongs in `internal/gateway/gateway_test.go`.
4. Run `gofmt -w .`, `go vet ./...` and `go test -race ./...` before pushing. CI runs the same checks on Windows, macOS and Linux.
5. Follow the MCP specification. When behaviour differs between protocol versions, link the relevant spec section in your PR.

## Ground rules

- No new dependencies without discussion. Junction ships as a single binary on purpose.
- Never log or record raw secrets. Anything written to recordings goes through the redactor.
- Be kind. See [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).
