# Security policy

Junction sits between AI clients and MCP servers, so security bugs matter.

## Reporting a vulnerability

Please **don't open a public issue**. Report it privately through
[GitHub's private vulnerability reporting](https://github.com/harshmangalamv/the-junction/security/advisories/new).

Include what you found, how to reproduce it, and the impact you expect. You'll get an acknowledgement within a few days. Once a fix is released, the advisory is published with credit to you, unless you'd rather stay anonymous.

## Supported versions

Junction is pre-1.0: security fixes go into the latest release only.

## Scope

In scope: anything that lets a client bypass policy or authentication, leaks data that redaction should have removed, lets a web page reach the gateway (DNS rebinding, origin checks), or crashes the gateway with crafted input.

Out of scope: vulnerabilities in the upstream MCP servers you put behind Junction.
