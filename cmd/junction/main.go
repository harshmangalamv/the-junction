// Command junction is an MCP gateway: one endpoint in front of many MCP
// servers, speaking both the legacy (2025-11-25) and the new stateless
// (2026-07-28) protocol, with policy, PII redaction, recording, replay and
// a live dashboard.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/harshmangalamv/the-junction/internal/config"
	"github.com/harshmangalamv/the-junction/internal/dashboard"
	"github.com/harshmangalamv/the-junction/internal/demo"
	"github.com/harshmangalamv/the-junction/internal/gateway"
	"github.com/harshmangalamv/the-junction/internal/recorder"
	"github.com/harshmangalamv/the-junction/internal/replay"
)

var version = "dev"

const usage = `Junction — an MCP gateway

Usage:
  junction demo                 Start with built-in demo servers, fake traffic and the dashboard
  junction serve  [-c file]     Run the gateway over HTTP (MCP endpoint at /mcp, dashboard at /)
  junction stdio  [-c file]     Run the gateway over stdin/stdout (for desktop MCP clients)
  junction init   [-o file]     Write a starter junction.yaml
  junction tools  [-c file]     List aggregated tools with token cost and policy decisions
  junction replay [-c file] -f recording.jsonl
                                Re-send recorded tool calls and report results that changed
  junction version

Run "junction <command> -h" for a command's flags.
`

func main() {
	gateway.Version = version
	args := os.Args[1:]
	if len(args) == 0 {
		// Double-clicked or run bare: use ./junction.yaml if present, else the demo.
		if _, err := os.Stat("junction.yaml"); err == nil {
			args = []string{"serve", "--open"}
		} else {
			args = []string{"demo"}
		}
	}
	var err error
	switch args[0] {
	case "serve":
		err = cmdServe(args[1:])
	case "demo":
		err = cmdDemo(args[1:])
	case "stdio":
		err = cmdStdio(args[1:])
	case "init":
		err = cmdInit(args[1:])
	case "tools":
		err = cmdTools(args[1:])
	case "replay":
		err = cmdReplay(args[1:])
	case "demo-server":
		err = cmdDemoServer(args[1:])
	case "version", "--version", "-v":
		fmt.Println("junction", version)
	case "help", "-h", "--help":
		fmt.Print(usage)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n%s", args[0], usage)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func loadConfig(path string) (*config.Config, error) {
	cfg, err := config.Load(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("%s not found — run `junction init` to create one, or `junction demo` to try it out", path)
	}
	return cfg, err
}

func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

func cmdServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	path := fs.String("c", "junction.yaml", "config file")
	open := fs.Bool("open", false, "open the dashboard in a browser")
	listen := fs.String("listen", "", "override listen address")
	fs.Parse(args)
	cfg, err := loadConfig(*path)
	if err != nil {
		return err
	}
	if *listen != "" {
		cfg.Listen = *listen
	}
	return run(cfg, *open, false)
}

func cmdDemo(args []string) error {
	fs := flag.NewFlagSet("demo", flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:7777", "listen address")
	noTraffic := fs.Bool("no-traffic", false, "don't generate simulated client traffic")
	noOpen := fs.Bool("no-open", false, "don't open a browser")
	fs.Parse(args)
	cfg, err := config.Parse(config.Example, "")
	if err != nil {
		return err
	}
	cfg.Listen = *listen
	return run(cfg, !*noOpen, !*noTraffic)
}

func run(cfg *config.Config, open, traffic bool) error {
	logger := log.New(os.Stderr, "", log.Ltime)
	g, err := gateway.New(cfg, logger)
	if err != nil {
		return err
	}
	defer g.Close()

	ctx, stop := signalContext()
	defer stop()

	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return fmt.Errorf("cannot listen on %s: %w (is another Junction running? try --listen 127.0.0.1:7788)", cfg.Listen, err)
	}
	cfg.Listen = ln.Addr().String()

	logger.Printf("Junction %s starting, connecting to %d servers…", version, len(cfg.Servers))
	g.Start(ctx)

	mux := http.NewServeMux()
	mux.Handle("/mcp", g)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok\n")) })
	if cfg.Dashboard.On() {
		g.RegisterAPI(mux)
		mux.Handle("/", dashboard.Handler())
	}
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}

	base := "http://" + cfg.Listen
	fmt.Fprintf(os.Stderr, "\n  MCP endpoint  %s/mcp\n", base)
	if cfg.Dashboard.On() {
		fmt.Fprintf(os.Stderr, "  Dashboard     %s/\n", base)
	}
	if f := cfg.Resolve(cfg.Record.File); f != "" {
		fmt.Fprintf(os.Stderr, "  Recording to  %s\n", f)
	}
	fmt.Fprintf(os.Stderr, "\n  Press Ctrl+C to stop.\n\n")

	go func() {
		<-ctx.Done()
		shut, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(shut)
	}()
	if traffic {
		var tok string
		if len(cfg.Auth.BearerTokens) > 0 {
			tok = cfg.Auth.BearerTokens[0]
		}
		go demo.SimulateTraffic(ctx, base+"/mcp", tok, logger)
	}
	if open && cfg.Dashboard.On() {
		go func() { time.Sleep(300 * time.Millisecond); openBrowser(base + "/") }()
	}
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	logger.Printf("stopped")
	return nil
}

func openBrowser(url string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	case "darwin":
		cmd = exec.Command("open", url)
	default:
		cmd = exec.Command("xdg-open", url)
	}
	_ = cmd.Start()
}

func cmdStdio(args []string) error {
	fs := flag.NewFlagSet("stdio", flag.ExitOnError)
	path := fs.String("c", "junction.yaml", "config file")
	dash := fs.Bool("dashboard", true, "also serve the dashboard on the configured listen address")
	fs.Parse(args)
	cfg, err := loadConfig(*path)
	if err != nil {
		return err
	}
	// stdout carries MCP messages, so all logging goes to stderr.
	logger := log.New(os.Stderr, "junction ", log.Ltime)
	g, err := gateway.New(cfg, logger)
	if err != nil {
		return err
	}
	defer g.Close()
	ctx, stop := signalContext()
	defer stop()
	g.Start(ctx)
	if *dash && cfg.Dashboard.On() {
		mux := http.NewServeMux()
		g.RegisterAPI(mux)
		mux.Handle("/", dashboard.Handler())
		go func() {
			if err := http.ListenAndServe(cfg.Listen, mux); err != nil {
				logger.Printf("dashboard not started: %v", err)
			}
		}()
		logger.Printf("dashboard at http://%s/", cfg.Listen)
	}
	return g.ServeStdio(ctx, os.Stdin, os.Stdout)
}

func cmdInit(args []string) error {
	fs := flag.NewFlagSet("init", flag.ExitOnError)
	out := fs.String("o", "junction.yaml", "output file")
	force := fs.Bool("force", false, "overwrite an existing file")
	fs.Parse(args)
	if _, err := os.Stat(*out); err == nil && !*force {
		return fmt.Errorf("%s already exists (use -force to overwrite)", *out)
	}
	if err := os.WriteFile(*out, config.Example, 0o644); err != nil {
		return err
	}
	fmt.Printf("Wrote %s. Start the gateway with: junction serve --open\n", *out)
	return nil
}

func cmdTools(args []string) error {
	fs := flag.NewFlagSet("tools", flag.ExitOnError)
	path := fs.String("c", "junction.yaml", "config file")
	fs.Parse(args)
	cfg, err := loadConfig(*path)
	if err != nil {
		return err
	}
	cfg.Record.File = ""
	g, err := gateway.New(cfg, log.New(os.Stderr, "", log.Ltime))
	if err != nil {
		return err
	}
	defer g.Close()
	ctx, stop := signalContext()
	defer stop()
	g.Start(ctx)

	fmt.Println()
	for _, s := range g.Servers() {
		state := "down"
		if s.Connected {
			state = s.Era.String() + " " + s.Version
		}
		fmt.Printf("  %-12s %-22s %d tools  %s\n", s.Name, state, s.Tools, s.LastError)
	}
	fmt.Println()
	tools := g.Tools()
	sort.SliceStable(tools, func(i, j int) bool { return tools[i].Tokens > tools[j].Tokens })
	tw := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "  TOOL\t~TOKENS\tVISIBLE\tRULE")
	for _, t := range tools {
		vis := "yes"
		if !t.Allowed {
			vis = "no"
		}
		fmt.Fprintf(tw, "  %s\t%d\t%s\t%s\n", t.Name, t.Tokens, vis, t.Rule)
	}
	tw.Flush()
	r := g.Tokens()
	fmt.Printf("\n  Clients receive %d tools costing ~%d tokens per request (%d hidden by policy, ~%d tokens saved).\n  Token counts are estimates (~4 characters per token).\n\n",
		r.VisibleTools, r.VisibleTokens, r.HiddenTools, r.HiddenTokens)
	return nil
}

type multiFlag []string

func (m *multiFlag) String() string     { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error { *m = append(*m, v); return nil }

func cmdReplay(args []string) error {
	fs := flag.NewFlagSet("replay", flag.ExitOnError)
	path := fs.String("c", "junction.yaml", "config file")
	file := fs.String("f", "recordings/traffic.jsonl", "recording to replay")
	tools := fs.String("tools", "", "only replay tools matching this glob, e.g. 'crm__*'")
	dedupe := fs.Bool("dedupe", true, "replay each distinct tool+arguments once")
	verbose := fs.Bool("v", false, "also list skipped calls")
	var ignore multiFlag
	fs.Var(&ignore, "ignore", "regex to blank before comparing (repeatable), e.g. '\\d{4}-\\d{2}-\\d{2}T[0-9:.]+Z'")
	fs.Parse(args)
	cfg, err := loadConfig(*path)
	if err != nil {
		return err
	}
	cfg.Record.File = "" // don't append replay traffic to the recording
	var res []*regexp.Regexp
	for _, p := range ignore {
		re, err := regexp.Compile(p)
		if err != nil {
			return fmt.Errorf("--ignore %q: %w", p, err)
		}
		res = append(res, re)
	}
	entries, err := recorder.ReadFile(*file)
	if err != nil {
		return err
	}
	g, err := gateway.New(cfg, log.New(os.Stderr, "", log.Ltime))
	if err != nil {
		return err
	}
	defer g.Close()
	ctx, stop := signalContext()
	defer stop()
	g.Start(ctx)
	rep := replay.Run(ctx, g, entries, replay.Options{Tools: *tools, Ignore: res, Dedupe: *dedupe})
	replay.Print(os.Stdout, rep, *verbose)
	if rep.Changed > 0 || rep.Errors > 0 {
		os.Exit(1)
	}
	return nil
}

func cmdDemoServer(args []string) error {
	fs := flag.NewFlagSet("demo-server", flag.ExitOnError)
	persona := fs.String("persona", "crm", "crm or billing")
	era := fs.String("era", "", "legacy, modern or dual (default depends on persona)")
	fs.Parse(args)
	return demo.Serve(os.Stdin, os.Stdout, demo.Options{Persona: *persona, Era: *era})
}
