package upstream

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"

	"github.com/harshmangalamv/the-junction/internal/jsonrpc"
)

// stdioTransport talks newline-delimited JSON-RPC to a child process.
type stdioTransport struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	writeM sync.Mutex

	mu      sync.Mutex
	pending map[string]chan *jsonrpc.Message
	closed  bool
	exitErr error
	done    chan struct{}

	onNotify func(*jsonrpc.Message)
	logs     *LogRing
}

func startStdio(command []string, env map[string]string, dir string, logs *LogRing, onNotify func(*jsonrpc.Message)) (*stdioTransport, error) {
	if len(command) == 0 {
		return nil, errors.New("empty command")
	}
	cmd := exec.Command(command[0], command[1:]...)
	cmd.Dir = dir
	cmd.Env = os.Environ()
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start %q: %w", command[0], err)
	}
	t := &stdioTransport{
		cmd: cmd, stdin: stdin,
		pending:  map[string]chan *jsonrpc.Message{},
		done:     make(chan struct{}),
		onNotify: onNotify, logs: logs,
	}
	go t.readStderr(stderr)
	go t.readLoop(stdout)
	return t, nil
}

func (t *stdioTransport) readStderr(r io.Reader) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		t.logs.Add("stderr", sc.Text())
	}
}

func (t *stdioTransport) readLoop(r io.Reader) {
	rd := bufio.NewReaderSize(r, 64*1024)
	for {
		line, err := rd.ReadBytes('\n')
		if len(line) > 0 {
			t.handleLine(line)
		}
		if err != nil {
			break
		}
	}
	waitErr := t.cmd.Wait()
	t.mu.Lock()
	t.closed = true
	if waitErr == nil {
		waitErr = errors.New("process exited")
	}
	t.exitErr = waitErr
	for id, ch := range t.pending {
		close(ch)
		delete(t.pending, id)
	}
	t.mu.Unlock()
	t.logs.Add("junction", "process exited: "+waitErr.Error())
	close(t.done)
}

func (t *stdioTransport) handleLine(line []byte) {
	if len(trimSpace(line)) == 0 {
		return
	}
	m, perr := jsonrpc.Parse(line)
	if perr != nil {
		t.logs.Add("stdout", "non-JSON-RPC output: "+truncate(string(line), 300))
		return
	}
	switch {
	case m.IsResponse():
		t.mu.Lock()
		ch, ok := t.pending[m.IDString()]
		if ok {
			delete(t.pending, m.IDString())
		}
		t.mu.Unlock()
		if ok {
			ch <- m
		}
	case m.IsRequest():
		// Server → client request (legacy servers only).
		t.logs.Add("junction", "server asked for "+m.Method)
		_ = t.write(serverRequestReply(m))
	case m.IsNotification():
		if t.onNotify != nil {
			t.onNotify(m)
		}
	}
}

func (t *stdioTransport) write(m *jsonrpc.Message) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	t.writeM.Lock()
	defer t.writeM.Unlock()
	_, err = t.stdin.Write(b)
	return err
}

func (t *stdioTransport) roundTrip(ctx context.Context, req *jsonrpc.Message, _ headers) (*jsonrpc.Message, error) {
	ch := make(chan *jsonrpc.Message, 1)
	t.mu.Lock()
	if t.closed {
		err := t.exitErr
		t.mu.Unlock()
		return nil, fmt.Errorf("server not running: %w", err)
	}
	t.pending[req.IDString()] = ch
	t.mu.Unlock()

	if err := t.write(req); err != nil {
		t.forget(req.IDString())
		return nil, err
	}
	select {
	case m, ok := <-ch:
		if !ok {
			return nil, fmt.Errorf("server exited during request: %w", t.exitErr)
		}
		return m, nil
	case <-ctx.Done():
		t.forget(req.IDString())
		// Tell the server to stop working on it (stdio cancellation).
		if n, err := jsonrpc.NewNotification("notifications/cancelled", map[string]any{
			"requestId": req.ID, "reason": "timeout or client cancelled",
		}); err == nil {
			_ = t.write(n)
		}
		return nil, ctx.Err()
	}
}

func (t *stdioTransport) forget(id string) {
	t.mu.Lock()
	delete(t.pending, id)
	t.mu.Unlock()
}

func (t *stdioTransport) notify(_ context.Context, m *jsonrpc.Message, _ headers) error {
	return t.write(m)
}

func (t *stdioTransport) close() error {
	_ = t.stdin.Close()
	select {
	case <-t.done:
	default:
		if t.cmd.Process != nil {
			_ = t.cmd.Process.Kill()
		}
	}
	return nil
}

func (t *stdioTransport) alive() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return !t.closed
}

func trimSpace(b []byte) []byte {
	i, j := 0, len(b)
	for i < j && (b[i] == ' ' || b[i] == '\n' || b[i] == '\r' || b[i] == '\t') {
		i++
	}
	for j > i && (b[j-1] == ' ' || b[j-1] == '\n' || b[j-1] == '\r' || b[j-1] == '\t') {
		j--
	}
	return b[i:j]
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
