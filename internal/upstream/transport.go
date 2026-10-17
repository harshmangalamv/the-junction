package upstream

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/harshmangalamv/the-junction/internal/jsonrpc"
)

// transport moves JSON-RPC messages to one upstream server.
type transport interface {
	// roundTrip sends a request and waits for its response. A JSON-RPC
	// error response is returned as a message, not as err.
	roundTrip(ctx context.Context, req *jsonrpc.Message, h headers) (*jsonrpc.Message, error)
	// notify sends a notification.
	notify(ctx context.Context, msg *jsonrpc.Message, h headers) error
	close() error
}

// headers carries per-request HTTP metadata. stdio ignores it.
type headers struct {
	protocolVersion string
	sessionID       string
	method          string
	name            string
}

// HTTPStatusError is returned when an HTTP upstream answers with a non-2xx
// status whose body is not a JSON-RPC message.
type HTTPStatusError struct {
	Status int
	Body   string
}

func (e *HTTPStatusError) Error() string {
	b := e.Body
	if len(b) > 200 {
		b = b[:200] + "…"
	}
	return fmt.Sprintf("upstream HTTP %d: %s", e.Status, b)
}

// serverRequestHandler answers requests an upstream sends to Junction
// (legacy servers may ask for roots, sampling or elicitation).
func serverRequestReply(m *jsonrpc.Message) *jsonrpc.Message {
	switch m.Method {
	case "ping":
		return jsonrpc.NewResult(m.ID, json.RawMessage(`{}`))
	case "roots/list":
		return jsonrpc.NewResult(m.ID, json.RawMessage(`{"roots":[]}`))
	default:
		return jsonrpc.NewErrorResponse(m.ID, jsonrpc.NewError(jsonrpc.CodeMethodNotFound,
			"junction: server-initiated "+m.Method+" is not bridged yet", nil))
	}
}
