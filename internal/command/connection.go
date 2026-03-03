package command

func registerConnectionCommands(r *Registry) {
	r.Register("PING", cmdPing, CommandInfo{Name: "PING", Arity: -1})
	r.Register("SELECT", cmdSelect, CommandInfo{Name: "SELECT", Arity: 2})
	r.Register("QUIT", cmdQuit, CommandInfo{Name: "QUIT", Arity: 1})
	r.Register("ECHO", cmdEcho, CommandInfo{Name: "ECHO", Arity: 2})
	r.Register("AUTH", cmdAuth, CommandInfo{Name: "AUTH", Arity: 2})
	r.Register("CLIENT", cmdClient, CommandInfo{Name: "CLIENT", Arity: -2})
}

func cmdPing(ctx *Context, args []string) error {
	if len(args) > 0 {
		return ctx.Writer.WriteBulkString([]byte(args[0]))
	}
	return ctx.Writer.WriteSimpleString("PONG")
}

func cmdSelect(ctx *Context, args []string) error {
	if len(args) < 1 {
		return ctx.Writer.WriteError("wrong number of arguments for 'select'")
	}
	var idx int
	for _, c := range args[0] {
		if c < '0' || c > '9' {
			return ctx.Writer.WriteError("value is not an integer or out of range")
		}
		idx = idx*10 + int(c-'0')
	}
	if err := ctx.Backend.SwitchBucket(ctx.Ctx, idx); err != nil {
		return ctx.Writer.WriteError(err.Error())
	}
	ctx.ConnState.SelectedDB = idx
	return ctx.Writer.WriteSimpleString("OK")
}

func cmdQuit(ctx *Context, _ []string) error {
	ctx.Writer.WriteSimpleString("OK")
	ctx.Writer.Flush()
	return errQuit
}

// errQuit is a sentinel error to signal the connection should close.
var errQuit = &quitError{}

type quitError struct{}

func (e *quitError) Error() string { return "quit" }

// IsQuitError returns true if the error is a quit signal.
func IsQuitError(err error) bool {
	_, ok := err.(*quitError)
	return ok
}

func cmdEcho(ctx *Context, args []string) error {
	if len(args) < 1 {
		return ctx.Writer.WriteError("wrong number of arguments for 'echo'")
	}
	return ctx.Writer.WriteBulkString([]byte(args[0]))
}

func cmdAuth(ctx *Context, args []string) error {
	if len(args) < 1 {
		return ctx.Writer.WriteError("wrong number of arguments for 'auth'")
	}
	// Auth is handled at the server level; always succeed here
	ctx.ConnState.Authenticated = true
	return ctx.Writer.WriteSimpleString("OK")
}

func cmdClient(ctx *Context, args []string) error {
	if len(args) < 1 {
		return ctx.Writer.WriteError("wrong number of arguments for 'client'")
	}
	switch {
	case len(args) >= 2 && (args[0] == "SETNAME" || args[0] == "setname"):
		ctx.ConnState.ClientName = args[1]
		return ctx.Writer.WriteSimpleString("OK")
	case args[0] == "GETNAME" || args[0] == "getname":
		if ctx.ConnState.ClientName == "" {
			return ctx.Writer.WriteNil()
		}
		return ctx.Writer.WriteBulkString([]byte(ctx.ConnState.ClientName))
	default:
		return ctx.Writer.WriteError("unknown subcommand '" + args[0] + "'")
	}
}
