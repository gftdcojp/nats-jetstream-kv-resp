package command

import (
	"strconv"
	"strings"
	"time"

	"github.com/gftdcojp/nats-jetstream-kv-resp/internal/store"
)

func registerStringCommands(r *Registry) {
	r.Register("GET", cmdGet, CommandInfo{Name: "GET", Arity: 2})
	r.Register("SET", cmdSet, CommandInfo{Name: "SET", Arity: -3})
	r.Register("MGET", cmdMGet, CommandInfo{Name: "MGET", Arity: -2})
	r.Register("MSET", cmdMSet, CommandInfo{Name: "MSET", Arity: -3})
	r.Register("SETNX", cmdSetNX, CommandInfo{Name: "SETNX", Arity: 3})
	r.Register("APPEND", cmdAppend, CommandInfo{Name: "APPEND", Arity: 3})
	r.Register("GETSET", cmdGetSet, CommandInfo{Name: "GETSET", Arity: 3})
	r.Register("STRLEN", cmdStrlen, CommandInfo{Name: "STRLEN", Arity: 2})
	r.Register("SETEX", cmdSetEX, CommandInfo{Name: "SETEX", Arity: 4})
	r.Register("PSETEX", cmdPSetEX, CommandInfo{Name: "PSETEX", Arity: 4})
	r.Register("GETDEL", cmdGetDel, CommandInfo{Name: "GETDEL", Arity: 2})
}

func cmdGet(ctx *Context, args []string) error {
	if len(args) < 1 {
		return ctx.Writer.WriteError("wrong number of arguments for 'get'")
	}
	if ctx.TTL != nil && ctx.TTL.IsExpired(args[0]) {
		ctx.TTL.CleanupExpired(ctx.Ctx, ctx.Backend, args[0])
		return ctx.Writer.WriteNil()
	}
	val, err := ctx.Backend.Get(ctx.Ctx, args[0])
	if err != nil {
		if err == store.ErrKeyNotFound {
			return ctx.Writer.WriteNil()
		}
		return ctx.Writer.WriteError(err.Error())
	}
	return ctx.Writer.WriteBulkString(val)
}

func cmdSet(ctx *Context, args []string) error {
	if len(args) < 2 {
		return ctx.Writer.WriteError("wrong number of arguments for 'set'")
	}
	key, value := args[0], args[1]

	var nx, xx bool
	var expiry time.Duration

	for i := 2; i < len(args); i++ {
		switch strings.ToUpper(args[i]) {
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "EX":
			if i+1 >= len(args) {
				return ctx.Writer.WriteError("syntax error")
			}
			i++
			secs, err := strconv.ParseInt(args[i], 10, 64)
			if err != nil || secs <= 0 {
				return ctx.Writer.WriteError("invalid expire time in 'set'")
			}
			expiry = time.Duration(secs) * time.Second
		case "PX":
			if i+1 >= len(args) {
				return ctx.Writer.WriteError("syntax error")
			}
			i++
			ms, err := strconv.ParseInt(args[i], 10, 64)
			if err != nil || ms <= 0 {
				return ctx.Writer.WriteError("invalid expire time in 'set'")
			}
			expiry = time.Duration(ms) * time.Millisecond
		default:
			return ctx.Writer.WriteError("syntax error")
		}
	}

	if nx {
		_, err := ctx.Backend.Create(ctx.Ctx, key, []byte(value))
		if err != nil {
			if err == store.ErrKeyExists {
				return ctx.Writer.WriteNil()
			}
			return ctx.Writer.WriteError(err.Error())
		}
		if expiry > 0 && ctx.TTL != nil {
			ctx.TTL.SetExpiry(ctx.Ctx, ctx.Backend, key, expiry)
		}
		return ctx.Writer.WriteSimpleString("OK")
	}

	if xx {
		_, err := ctx.Backend.Get(ctx.Ctx, key)
		if err != nil {
			if err == store.ErrKeyNotFound {
				return ctx.Writer.WriteNil()
			}
			return ctx.Writer.WriteError(err.Error())
		}
	}

	if _, err := ctx.Backend.Put(ctx.Ctx, key, []byte(value)); err != nil {
		return ctx.Writer.WriteError(err.Error())
	}
	if expiry > 0 && ctx.TTL != nil {
		ctx.TTL.SetExpiry(ctx.Ctx, ctx.Backend, key, expiry)
	}
	return ctx.Writer.WriteSimpleString("OK")
}

func cmdMGet(ctx *Context, args []string) error {
	if len(args) < 1 {
		return ctx.Writer.WriteError("wrong number of arguments for 'mget'")
	}
	if err := ctx.Writer.WriteArrayLen(len(args)); err != nil {
		return err
	}
	for _, key := range args {
		if ctx.TTL != nil && ctx.TTL.IsExpired(key) {
			ctx.TTL.CleanupExpired(ctx.Ctx, ctx.Backend, key)
			if err := ctx.Writer.WriteNil(); err != nil {
				return err
			}
			continue
		}
		val, err := ctx.Backend.Get(ctx.Ctx, key)
		if err != nil {
			if err := ctx.Writer.WriteNil(); err != nil {
				return err
			}
			continue
		}
		if err := ctx.Writer.WriteBulkString(val); err != nil {
			return err
		}
	}
	return nil
}

func cmdMSet(ctx *Context, args []string) error {
	if len(args) < 2 || len(args)%2 != 0 {
		return ctx.Writer.WriteError("wrong number of arguments for 'mset'")
	}
	for i := 0; i < len(args); i += 2 {
		if _, err := ctx.Backend.Put(ctx.Ctx, args[i], []byte(args[i+1])); err != nil {
			return ctx.Writer.WriteError(err.Error())
		}
	}
	return ctx.Writer.WriteSimpleString("OK")
}

func cmdSetNX(ctx *Context, args []string) error {
	if len(args) < 2 {
		return ctx.Writer.WriteError("wrong number of arguments for 'setnx'")
	}
	_, err := ctx.Backend.Create(ctx.Ctx, args[0], []byte(args[1]))
	if err != nil {
		if err == store.ErrKeyExists {
			return ctx.Writer.WriteInteger(0)
		}
		return ctx.Writer.WriteError(err.Error())
	}
	return ctx.Writer.WriteInteger(1)
}

func cmdAppend(ctx *Context, args []string) error {
	if len(args) < 2 {
		return ctx.Writer.WriteError("wrong number of arguments for 'append'")
	}
	existing, err := ctx.Backend.Get(ctx.Ctx, args[0])
	if err != nil && err != store.ErrKeyNotFound {
		return ctx.Writer.WriteError(err.Error())
	}
	newVal := append(existing, []byte(args[1])...)
	if _, err := ctx.Backend.Put(ctx.Ctx, args[0], newVal); err != nil {
		return ctx.Writer.WriteError(err.Error())
	}
	return ctx.Writer.WriteInteger(int64(len(newVal)))
}

func cmdGetSet(ctx *Context, args []string) error {
	if len(args) < 2 {
		return ctx.Writer.WriteError("wrong number of arguments for 'getset'")
	}
	old, err := ctx.Backend.Get(ctx.Ctx, args[0])
	if err != nil && err != store.ErrKeyNotFound {
		return ctx.Writer.WriteError(err.Error())
	}
	if _, err := ctx.Backend.Put(ctx.Ctx, args[0], []byte(args[1])); err != nil {
		return ctx.Writer.WriteError(err.Error())
	}
	if old == nil {
		return ctx.Writer.WriteNil()
	}
	return ctx.Writer.WriteBulkString(old)
}

func cmdStrlen(ctx *Context, args []string) error {
	if len(args) < 1 {
		return ctx.Writer.WriteError("wrong number of arguments for 'strlen'")
	}
	val, err := ctx.Backend.Get(ctx.Ctx, args[0])
	if err != nil {
		if err == store.ErrKeyNotFound {
			return ctx.Writer.WriteInteger(0)
		}
		return ctx.Writer.WriteError(err.Error())
	}
	return ctx.Writer.WriteInteger(int64(len(val)))
}

func cmdSetEX(ctx *Context, args []string) error {
	if len(args) < 3 {
		return ctx.Writer.WriteError("wrong number of arguments for 'setex'")
	}
	secs, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil || secs <= 0 {
		return ctx.Writer.WriteError("invalid expire time in 'setex'")
	}
	if _, err := ctx.Backend.Put(ctx.Ctx, args[0], []byte(args[2])); err != nil {
		return ctx.Writer.WriteError(err.Error())
	}
	if ctx.TTL != nil {
		ctx.TTL.SetExpiry(ctx.Ctx, ctx.Backend, args[0], time.Duration(secs)*time.Second)
	}
	return ctx.Writer.WriteSimpleString("OK")
}

func cmdPSetEX(ctx *Context, args []string) error {
	if len(args) < 3 {
		return ctx.Writer.WriteError("wrong number of arguments for 'psetex'")
	}
	ms, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil || ms <= 0 {
		return ctx.Writer.WriteError("invalid expire time in 'psetex'")
	}
	if _, err := ctx.Backend.Put(ctx.Ctx, args[0], []byte(args[2])); err != nil {
		return ctx.Writer.WriteError(err.Error())
	}
	if ctx.TTL != nil {
		ctx.TTL.SetExpiry(ctx.Ctx, ctx.Backend, args[0], time.Duration(ms)*time.Millisecond)
	}
	return ctx.Writer.WriteSimpleString("OK")
}

func cmdGetDel(ctx *Context, args []string) error {
	if len(args) < 1 {
		return ctx.Writer.WriteError("wrong number of arguments for 'getdel'")
	}
	val, err := ctx.Backend.Get(ctx.Ctx, args[0])
	if err != nil {
		if err == store.ErrKeyNotFound {
			return ctx.Writer.WriteNil()
		}
		return ctx.Writer.WriteError(err.Error())
	}
	ctx.Backend.Delete(ctx.Ctx, args[0])
	if ctx.TTL != nil {
		ctx.TTL.RemoveExpiry(ctx.Ctx, ctx.Backend, args[0])
	}
	return ctx.Writer.WriteBulkString(val)
}
