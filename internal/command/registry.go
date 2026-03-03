package command

import (
	"context"
	"strings"
	"time"

	"github.com/gftdcojp/nats-jetstream-kv-resp/internal/resp"
	"github.com/gftdcojp/nats-jetstream-kv-resp/internal/store"
	"github.com/gftdcojp/nats-jetstream-kv-resp/internal/ttl"
)

// Handler processes a single Redis command.
type Handler func(ctx *Context, args []string) error

// Context holds per-command execution state.
type Context struct {
	Ctx       context.Context
	Writer    *resp.Writer
	Backend   store.Backend
	TTL       *ttl.Tracker
	ConnState *ConnState
	StartTime time.Time
}

// ConnState tracks per-connection mutable state.
type ConnState struct {
	SelectedDB    int
	ClientName    string
	Authenticated bool
}

// CommandInfo describes a registered command.
type CommandInfo struct {
	Name  string
	Arity int // positive = exact, negative = minimum (abs)
}

// Registry maps command names to handlers.
type Registry struct {
	handlers map[string]Handler
	info     map[string]CommandInfo
}

func NewRegistry() *Registry {
	r := &Registry{
		handlers: make(map[string]Handler),
		info:     make(map[string]CommandInfo),
	}
	r.registerAll()
	return r
}

func (r *Registry) Register(name string, handler Handler, info CommandInfo) {
	upper := strings.ToUpper(name)
	r.handlers[upper] = handler
	r.info[upper] = info
}

func (r *Registry) Execute(ctx *Context, args []string) error {
	if len(args) == 0 {
		return ctx.Writer.WriteError("empty command")
	}
	cmd := strings.ToUpper(args[0])
	h, ok := r.handlers[cmd]
	if !ok {
		return ctx.Writer.WriteError("unknown command '" + strings.ToLower(cmd) + "'")
	}
	return h(ctx, args[1:])
}

func (r *Registry) CommandCount() int {
	return len(r.handlers)
}

func (r *Registry) registerAll() {
	registerStringCommands(r)
	registerKeyCommands(r)
	registerHashCommands(r)
	registerListCommands(r)
	registerConnectionCommands(r)
	registerServerCommands(r)
}
