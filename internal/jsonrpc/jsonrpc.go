// Package jsonrpc implements the small subset of JSON-RPC 2.0 that MCP uses.
//
// IDs, params and results are kept as raw JSON so the gateway can forward
// messages without losing fields it does not understand.
package jsonrpc

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Standard JSON-RPC and MCP error codes.
const (
	CodeParseError     = -32700
	CodeInvalidRequest = -32600
	CodeMethodNotFound = -32601
	CodeInvalidParams  = -32602
	CodeInternalError  = -32603

	// Codes reserved by MCP 2026-07-28 (range -32020..-32099).
	CodeHeaderMismatch                  = -32020
	CodeMissingRequiredClientCapability = -32021
	CodeUnsupportedProtocolVersion      = -32022
)

// IsModernErrorCode reports whether code is one of the protocol errors that
// only a 2026-07-28 (or later) server would send. Seeing one tells a client
// it is talking to a modern server.
func IsModernErrorCode(code int) bool {
	return code <= -32020 && code >= -32099
}

// Message is any JSON-RPC message: request, notification or response.
type Message struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *Error          `json:"error,omitempty"`
}

// Error is a JSON-RPC error object.
type Error struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *Error) Error() string { return fmt.Sprintf("jsonrpc error %d: %s", e.Code, e.Message) }

// NewError builds an error, marshalling data if it is not nil.
func NewError(code int, msg string, data any) *Error {
	e := &Error{Code: code, Message: msg}
	if data != nil {
		if b, err := json.Marshal(data); err == nil {
			e.Data = b
		}
	}
	return e
}

// IsRequest reports whether m is a request (has a method and an id).
func (m *Message) IsRequest() bool { return m.Method != "" && hasID(m.ID) }

// IsNotification reports whether m is a notification (method, no id).
func (m *Message) IsNotification() bool { return m.Method != "" && !hasID(m.ID) }

// IsResponse reports whether m is a response (no method).
func (m *Message) IsResponse() bool { return m.Method == "" && (m.Result != nil || m.Error != nil) }

func hasID(id json.RawMessage) bool { return len(id) > 0 && !bytes.Equal(id, []byte("null")) }

// IDString returns the id as a comparable string key.
func (m *Message) IDString() string { return string(bytes.TrimSpace(m.ID)) }

// NewRequest builds a request with a numeric id.
func NewRequest(id int64, method string, params any) (*Message, error) {
	m := &Message{JSONRPC: "2.0", ID: json.RawMessage(fmt.Sprint(id)), Method: method}
	if params != nil {
		b, err := toRaw(params)
		if err != nil {
			return nil, err
		}
		m.Params = b
	}
	return m, nil
}

// NewNotification builds a notification.
func NewNotification(method string, params any) (*Message, error) {
	m := &Message{JSONRPC: "2.0", Method: method}
	if params != nil {
		b, err := toRaw(params)
		if err != nil {
			return nil, err
		}
		m.Params = b
	}
	return m, nil
}

// NewResult builds a success response for id.
func NewResult(id json.RawMessage, result any) *Message {
	b, err := toRaw(result)
	if err != nil {
		return NewErrorResponse(id, NewError(CodeInternalError, "failed to encode result: "+err.Error(), nil))
	}
	return &Message{JSONRPC: "2.0", ID: id, Result: b}
}

// NewErrorResponse builds an error response for id (id may be nil → null).
func NewErrorResponse(id json.RawMessage, e *Error) *Message {
	if len(id) == 0 {
		id = json.RawMessage("null")
	}
	return &Message{JSONRPC: "2.0", ID: id, Error: e}
}

func toRaw(v any) (json.RawMessage, error) {
	switch t := v.(type) {
	case json.RawMessage:
		return t, nil
	case []byte:
		return t, nil
	}
	return json.Marshal(v)
}

// Parse decodes one message and validates its basic shape.
func Parse(b []byte) (*Message, *Error) {
	b = bytes.TrimSpace(b)
	if len(b) > 0 && b[0] == '[' {
		return nil, NewError(CodeInvalidRequest, "JSON-RPC batches are not supported", nil)
	}
	var m Message
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, NewError(CodeParseError, "parse error: "+err.Error(), nil)
	}
	if m.JSONRPC != "2.0" {
		return &m, NewError(CodeInvalidRequest, `jsonrpc must be "2.0"`, nil)
	}
	return &m, nil
}
