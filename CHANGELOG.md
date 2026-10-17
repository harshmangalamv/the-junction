# Changelog

All notable changes are listed here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.1.0]

First public release.

### Added

- Dual-era gateway: serves MCP clients on `2025-11-25` (and earlier) and `2026-07-28` from one endpoint, and bridges them to upstream servers of either version.
- Upstream era detection via `server/discover` probing with fallback to `initialize`, legacy session renewal, and automatic restart of crashed stdio servers.
- Aggregation of tools, prompts and resources from many servers, namespaced as `server__tool`.
- Tool policy: allow/deny globs and an option to hide tools annotated `destructiveHint`.
- PII and secret redaction for responses and recordings, with custom regex rules.
- JSONL recording of every exchange, and `junction replay` to diff recorded tool calls against current servers.
- `junction tools` token report.
- Live web dashboard, HTTP and stdio front ends, bearer-token auth and origin checks.
- Built-in demo servers and `junction demo`.

[Unreleased]: https://github.com/harshmangalamv/the-junction/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/harshmangalamv/the-junction/releases/tag/v0.1.0
