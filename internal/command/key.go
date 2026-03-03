package command

import (
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gftdcojp/nats-jetstream-kv-resp/internal/store"
)

func registerKeyCommands(r *Registry) {
	r.Register("DEL", cmdDel, CommandInfo{Name: "DEL", Arity: -2})
	r.Register("EXISTS", cmdExists, CommandInfo{Name: "EXISTS", Arity: -2})
	r.Register("KEYS", cmdKeys, CommandInfo{Name: "KEYS", Arity: 2})
	r.Register("SCAN", cmdScan, CommandInfo{Name: "SCAN", Arity: -2})
	r.Register("TYPE", cmdType, CommandInfo{Name: "TYPE", Arity: 2})
	r.Register("RENAME", cmdRename, CommandInfo{Name: "RENAME", Arity: 3})
	r.Register("EXPIRE", cmdExpire, CommandInfo{Name: "EXPIRE", Arity: 3})
	r.Register("TTL", cmdTTLCmd, CommandInfo{Name: "TTL", Arity: 2})
	r.Register("PEXPIRE", cmdPExpire, CommandInfo{Name: "PEXPIRE", Arity: 3})
	r.Register("PTTL", cmdPTTL, CommandInfo{Name: "PTTL", Arity: 2})
	r.Register("PERSIST", cmdPersist, CommandInfo{Name: "PERSIST", Arity: 2})
	r.Register("DBSIZE", cmdDBSize, CommandInfo{Name: "DBSIZE", Arity: 1})
	r.Register("FLUSHDB", cmdFlushDB, CommandInfo{Name: "FLUSHDB", Arity: 1})
}

// internalPrefix returns true if the key is an internal metadata key.
func internalPrefix(key string) bool {
	return strings.HasPrefix(key, "_h.") ||
		strings.HasPrefix(key, "_hm.") ||
		strings.HasPrefix(key, "_l.") ||
		strings.HasPrefix(key, "_ttl.")
}

func cmdDel(ctx *Context, args []string) error {
	if len(args) < 1 {
		return ctx.Writer.WriteError("wrong number of arguments for 'del'")
	}
	var count int64
	for _, key := range args {
		_, err := ctx.Backend.Get(ctx.Ctx, key)
		if err == nil {
			if err := ctx.Backend.Delete(ctx.Ctx, key); err == nil {
				count++
				if ctx.TTL != nil {
					ctx.TTL.RemoveExpiry(ctx.Ctx, ctx.Backend, key)
				}
			}
		}
	}
	return ctx.Writer.WriteInteger(count)
}

func cmdExists(ctx *Context, args []string) error {
	if len(args) < 1 {
		return ctx.Writer.WriteError("wrong number of arguments for 'exists'")
	}
	var count int64
	for _, key := range args {
		if ctx.TTL != nil && ctx.TTL.IsExpired(key) {
			ctx.TTL.CleanupExpired(ctx.Ctx, ctx.Backend, key)
			continue
		}
		_, err := ctx.Backend.Get(ctx.Ctx, key)
		if err == nil {
			count++
		}
	}
	return ctx.Writer.WriteInteger(count)
}

func cmdKeys(ctx *Context, args []string) error {
	if len(args) < 1 {
		return ctx.Writer.WriteError("wrong number of arguments for 'keys'")
	}
	pattern := args[0]
	allKeys, err := ctx.Backend.Keys(ctx.Ctx)
	if err != nil {
		return ctx.Writer.WriteError(err.Error())
	}

	var matched []string
	for _, key := range allKeys {
		if internalPrefix(key) {
			continue
		}
		if ctx.TTL != nil && ctx.TTL.IsExpired(key) {
			continue
		}
		if matchRedisPattern(pattern, key) {
			matched = append(matched, key)
		}
	}

	if err := ctx.Writer.WriteArrayLen(len(matched)); err != nil {
		return err
	}
	for _, key := range matched {
		if err := ctx.Writer.WriteBulkString([]byte(key)); err != nil {
			return err
		}
	}
	return nil
}

func cmdScan(ctx *Context, args []string) error {
	if len(args) < 1 {
		return ctx.Writer.WriteError("wrong number of arguments for 'scan'")
	}
	cursor, _ := strconv.Atoi(args[0])
	pattern := "*"
	count := 10

	for i := 1; i+1 < len(args); i += 2 {
		switch strings.ToUpper(args[i]) {
		case "MATCH":
			pattern = args[i+1]
		case "COUNT":
			c, err := strconv.Atoi(args[i+1])
			if err == nil && c > 0 {
				count = c
			}
		}
	}

	allKeys, err := ctx.Backend.Keys(ctx.Ctx)
	if err != nil {
		return ctx.Writer.WriteError(err.Error())
	}

	// Filter and sort
	var filtered []string
	for _, key := range allKeys {
		if internalPrefix(key) {
			continue
		}
		if ctx.TTL != nil && ctx.TTL.IsExpired(key) {
			continue
		}
		if matchRedisPattern(pattern, key) {
			filtered = append(filtered, key)
		}
	}
	sort.Strings(filtered)

	// Paginate
	start := cursor
	if start > len(filtered) {
		start = len(filtered)
	}
	end := start + count
	if end > len(filtered) {
		end = len(filtered)
	}

	nextCursor := 0
	if end < len(filtered) {
		nextCursor = end
	}

	// Write array of 2: [cursor, [keys...]]
	if err := ctx.Writer.WriteArrayLen(2); err != nil {
		return err
	}
	if err := ctx.Writer.WriteBulkString([]byte(strconv.Itoa(nextCursor))); err != nil {
		return err
	}
	page := filtered[start:end]
	if err := ctx.Writer.WriteArrayLen(len(page)); err != nil {
		return err
	}
	for _, key := range page {
		if err := ctx.Writer.WriteBulkString([]byte(key)); err != nil {
			return err
		}
	}
	return nil
}

func cmdType(ctx *Context, args []string) error {
	if len(args) < 1 {
		return ctx.Writer.WriteError("wrong number of arguments for 'type'")
	}
	key := args[0]

	// Check hash metadata
	_, err := ctx.Backend.Get(ctx.Ctx, "_hm."+key)
	if err == nil {
		return ctx.Writer.WriteSimpleString("hash")
	}
	// Check list
	_, err = ctx.Backend.Get(ctx.Ctx, "_l."+key)
	if err == nil {
		return ctx.Writer.WriteSimpleString("list")
	}
	// Check string
	_, err = ctx.Backend.Get(ctx.Ctx, key)
	if err == nil {
		return ctx.Writer.WriteSimpleString("string")
	}
	return ctx.Writer.WriteSimpleString("none")
}

func cmdRename(ctx *Context, args []string) error {
	if len(args) < 2 {
		return ctx.Writer.WriteError("wrong number of arguments for 'rename'")
	}
	val, err := ctx.Backend.Get(ctx.Ctx, args[0])
	if err != nil {
		if err == store.ErrKeyNotFound {
			return ctx.Writer.WriteError("no such key")
		}
		return ctx.Writer.WriteError(err.Error())
	}
	if _, err := ctx.Backend.Put(ctx.Ctx, args[1], val); err != nil {
		return ctx.Writer.WriteError(err.Error())
	}
	ctx.Backend.Delete(ctx.Ctx, args[0])
	// Migrate TTL
	if ctx.TTL != nil {
		remaining := ctx.TTL.GetTTLMillis(args[0])
		ctx.TTL.RemoveExpiry(ctx.Ctx, ctx.Backend, args[0])
		if remaining > 0 {
			ctx.TTL.SetExpiry(ctx.Ctx, ctx.Backend, args[1], time.Duration(remaining)*time.Millisecond)
		}
	}
	return ctx.Writer.WriteSimpleString("OK")
}

func cmdExpire(ctx *Context, args []string) error {
	if len(args) < 2 {
		return ctx.Writer.WriteError("wrong number of arguments for 'expire'")
	}
	secs, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil || secs <= 0 {
		return ctx.Writer.WriteError("invalid expire time")
	}
	_, gerr := ctx.Backend.Get(ctx.Ctx, args[0])
	if gerr != nil {
		return ctx.Writer.WriteInteger(0)
	}
	if ctx.TTL != nil {
		ctx.TTL.SetExpiry(ctx.Ctx, ctx.Backend, args[0], time.Duration(secs)*time.Second)
	}
	return ctx.Writer.WriteInteger(1)
}

func cmdTTLCmd(ctx *Context, args []string) error {
	if len(args) < 1 {
		return ctx.Writer.WriteError("wrong number of arguments for 'ttl'")
	}
	_, err := ctx.Backend.Get(ctx.Ctx, args[0])
	if err != nil {
		return ctx.Writer.WriteInteger(-2) // key does not exist
	}
	if ctx.TTL == nil {
		return ctx.Writer.WriteInteger(-1) // no expiry
	}
	ms := ctx.TTL.GetTTLMillis(args[0])
	if ms < 0 {
		return ctx.Writer.WriteInteger(-1)
	}
	return ctx.Writer.WriteInteger(ms / 1000)
}

func cmdPExpire(ctx *Context, args []string) error {
	if len(args) < 2 {
		return ctx.Writer.WriteError("wrong number of arguments for 'pexpire'")
	}
	ms, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil || ms <= 0 {
		return ctx.Writer.WriteError("invalid expire time")
	}
	_, gerr := ctx.Backend.Get(ctx.Ctx, args[0])
	if gerr != nil {
		return ctx.Writer.WriteInteger(0)
	}
	if ctx.TTL != nil {
		ctx.TTL.SetExpiry(ctx.Ctx, ctx.Backend, args[0], time.Duration(ms)*time.Millisecond)
	}
	return ctx.Writer.WriteInteger(1)
}

func cmdPTTL(ctx *Context, args []string) error {
	if len(args) < 1 {
		return ctx.Writer.WriteError("wrong number of arguments for 'pttl'")
	}
	_, err := ctx.Backend.Get(ctx.Ctx, args[0])
	if err != nil {
		return ctx.Writer.WriteInteger(-2)
	}
	if ctx.TTL == nil {
		return ctx.Writer.WriteInteger(-1)
	}
	ms := ctx.TTL.GetTTLMillis(args[0])
	if ms < 0 {
		return ctx.Writer.WriteInteger(-1)
	}
	return ctx.Writer.WriteInteger(ms)
}

func cmdPersist(ctx *Context, args []string) error {
	if len(args) < 1 {
		return ctx.Writer.WriteError("wrong number of arguments for 'persist'")
	}
	if ctx.TTL == nil {
		return ctx.Writer.WriteInteger(0)
	}
	ms := ctx.TTL.GetTTLMillis(args[0])
	if ms < 0 {
		return ctx.Writer.WriteInteger(0)
	}
	ctx.TTL.RemoveExpiry(ctx.Ctx, ctx.Backend, args[0])
	return ctx.Writer.WriteInteger(1)
}

func cmdDBSize(ctx *Context, _ []string) error {
	allKeys, err := ctx.Backend.Keys(ctx.Ctx)
	if err != nil {
		return ctx.Writer.WriteError(err.Error())
	}
	var count int64
	for _, key := range allKeys {
		if !internalPrefix(key) {
			count++
		}
	}
	return ctx.Writer.WriteInteger(count)
}

func cmdFlushDB(ctx *Context, _ []string) error {
	if err := ctx.Backend.PurgeAll(ctx.Ctx); err != nil {
		return ctx.Writer.WriteError(err.Error())
	}
	return ctx.Writer.WriteSimpleString("OK")
}

// matchRedisPattern matches a Redis-style glob pattern against a string.
// Supports: * ? [abc] \.
func matchRedisPattern(pattern, s string) bool {
	ok, _ := filepath.Match(pattern, s)
	return ok
}
