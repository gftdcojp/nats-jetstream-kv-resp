package command

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"

	"github.com/gftdcojp/nats-jetstream-kv-resp/internal/store"
)

func registerHashCommands(r *Registry) {
	r.Register("HSET", cmdHSet, CommandInfo{Name: "HSET", Arity: -4})
	r.Register("HGET", cmdHGet, CommandInfo{Name: "HGET", Arity: 3})
	r.Register("HDEL", cmdHDel, CommandInfo{Name: "HDEL", Arity: -3})
	r.Register("HGETALL", cmdHGetAll, CommandInfo{Name: "HGETALL", Arity: 2})
	r.Register("HKEYS", cmdHKeys, CommandInfo{Name: "HKEYS", Arity: 2})
	r.Register("HVALS", cmdHVals, CommandInfo{Name: "HVALS", Arity: 2})
	r.Register("HLEN", cmdHLen, CommandInfo{Name: "HLEN", Arity: 2})
	r.Register("HMGET", cmdHMGet, CommandInfo{Name: "HMGET", Arity: -3})
	r.Register("HMSET", cmdHMSet, CommandInfo{Name: "HMSET", Arity: -4})
	r.Register("HEXISTS", cmdHExists, CommandInfo{Name: "HEXISTS", Arity: 3})
	r.Register("HSCAN", cmdHScan, CommandInfo{Name: "HSCAN", Arity: -3})
}

// hashMetaKey returns the metadata key for a hash.
func hashMetaKey(key string) string { return "_hm." + key }

// hashFieldKey returns the data key for a hash field.
func hashFieldKey(key, field string) string { return "_h." + key + "." + field }

// getHashFields loads the field list from metadata.
func getHashFields(ctx *Context, key string) ([]string, error) {
	data, err := ctx.Backend.Get(ctx.Ctx, hashMetaKey(key))
	if err != nil {
		if err == store.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}
	var fields []string
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, err
	}
	return fields, nil
}

// saveHashFields saves the field list to metadata.
func saveHashFields(ctx *Context, key string, fields []string) error {
	data, _ := json.Marshal(fields)
	_, err := ctx.Backend.Put(ctx.Ctx, hashMetaKey(key), data)
	return err
}

func cmdHSet(ctx *Context, args []string) error {
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		return ctx.Writer.WriteError("wrong number of arguments for 'hset'")
	}
	key := args[0]
	fields, _ := getHashFields(ctx, key)
	fieldSet := make(map[string]bool, len(fields))
	for _, f := range fields {
		fieldSet[f] = true
	}

	var newCount int64
	for i := 1; i < len(args); i += 2 {
		field, value := args[i], args[i+1]
		if _, err := ctx.Backend.Put(ctx.Ctx, hashFieldKey(key, field), []byte(value)); err != nil {
			return ctx.Writer.WriteError(err.Error())
		}
		if !fieldSet[field] {
			fieldSet[field] = true
			fields = append(fields, field)
			newCount++
		}
	}
	if err := saveHashFields(ctx, key, fields); err != nil {
		return ctx.Writer.WriteError(err.Error())
	}
	return ctx.Writer.WriteInteger(newCount)
}

func cmdHGet(ctx *Context, args []string) error {
	if len(args) < 2 {
		return ctx.Writer.WriteError("wrong number of arguments for 'hget'")
	}
	val, err := ctx.Backend.Get(ctx.Ctx, hashFieldKey(args[0], args[1]))
	if err != nil {
		if err == store.ErrKeyNotFound {
			return ctx.Writer.WriteNil()
		}
		return ctx.Writer.WriteError(err.Error())
	}
	return ctx.Writer.WriteBulkString(val)
}

func cmdHDel(ctx *Context, args []string) error {
	if len(args) < 2 {
		return ctx.Writer.WriteError("wrong number of arguments for 'hdel'")
	}
	key := args[0]
	fields, _ := getHashFields(ctx, key)

	delSet := make(map[string]bool, len(args)-1)
	for _, f := range args[1:] {
		delSet[f] = true
	}

	var count int64
	for _, f := range args[1:] {
		err := ctx.Backend.Delete(ctx.Ctx, hashFieldKey(key, f))
		if err == nil {
			count++
		}
	}

	// Update metadata
	var remaining []string
	for _, f := range fields {
		if !delSet[f] {
			remaining = append(remaining, f)
		}
	}
	if len(remaining) == 0 {
		ctx.Backend.Delete(ctx.Ctx, hashMetaKey(key))
	} else {
		saveHashFields(ctx, key, remaining)
	}
	return ctx.Writer.WriteInteger(count)
}

func cmdHGetAll(ctx *Context, args []string) error {
	if len(args) < 1 {
		return ctx.Writer.WriteError("wrong number of arguments for 'hgetall'")
	}
	fields, _ := getHashFields(ctx, args[0])
	if len(fields) == 0 {
		return ctx.Writer.WriteArrayLen(0)
	}
	if err := ctx.Writer.WriteArrayLen(len(fields) * 2); err != nil {
		return err
	}
	for _, f := range fields {
		if err := ctx.Writer.WriteBulkString([]byte(f)); err != nil {
			return err
		}
		val, err := ctx.Backend.Get(ctx.Ctx, hashFieldKey(args[0], f))
		if err != nil {
			if err := ctx.Writer.WriteNil(); err != nil {
				return err
			}
		} else {
			if err := ctx.Writer.WriteBulkString(val); err != nil {
				return err
			}
		}
	}
	return nil
}

func cmdHKeys(ctx *Context, args []string) error {
	if len(args) < 1 {
		return ctx.Writer.WriteError("wrong number of arguments for 'hkeys'")
	}
	fields, _ := getHashFields(ctx, args[0])
	if err := ctx.Writer.WriteArrayLen(len(fields)); err != nil {
		return err
	}
	for _, f := range fields {
		if err := ctx.Writer.WriteBulkString([]byte(f)); err != nil {
			return err
		}
	}
	return nil
}

func cmdHVals(ctx *Context, args []string) error {
	if len(args) < 1 {
		return ctx.Writer.WriteError("wrong number of arguments for 'hvals'")
	}
	fields, _ := getHashFields(ctx, args[0])
	if err := ctx.Writer.WriteArrayLen(len(fields)); err != nil {
		return err
	}
	for _, f := range fields {
		val, err := ctx.Backend.Get(ctx.Ctx, hashFieldKey(args[0], f))
		if err != nil {
			if err := ctx.Writer.WriteNil(); err != nil {
				return err
			}
		} else {
			if err := ctx.Writer.WriteBulkString(val); err != nil {
				return err
			}
		}
	}
	return nil
}

func cmdHLen(ctx *Context, args []string) error {
	if len(args) < 1 {
		return ctx.Writer.WriteError("wrong number of arguments for 'hlen'")
	}
	fields, _ := getHashFields(ctx, args[0])
	return ctx.Writer.WriteInteger(int64(len(fields)))
}

func cmdHMGet(ctx *Context, args []string) error {
	if len(args) < 2 {
		return ctx.Writer.WriteError("wrong number of arguments for 'hmget'")
	}
	key := args[0]
	if err := ctx.Writer.WriteArrayLen(len(args) - 1); err != nil {
		return err
	}
	for _, f := range args[1:] {
		val, err := ctx.Backend.Get(ctx.Ctx, hashFieldKey(key, f))
		if err != nil {
			if err := ctx.Writer.WriteNil(); err != nil {
				return err
			}
		} else {
			if err := ctx.Writer.WriteBulkString(val); err != nil {
				return err
			}
		}
	}
	return nil
}

func cmdHMSet(ctx *Context, args []string) error {
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		return ctx.Writer.WriteError("wrong number of arguments for 'hmset'")
	}
	// Reuse HSET logic but return OK
	key := args[0]
	fields, _ := getHashFields(ctx, key)
	fieldSet := make(map[string]bool, len(fields))
	for _, f := range fields {
		fieldSet[f] = true
	}
	for i := 1; i < len(args); i += 2 {
		field, value := args[i], args[i+1]
		if _, err := ctx.Backend.Put(ctx.Ctx, hashFieldKey(key, field), []byte(value)); err != nil {
			return ctx.Writer.WriteError(err.Error())
		}
		if !fieldSet[field] {
			fieldSet[field] = true
			fields = append(fields, field)
		}
	}
	saveHashFields(ctx, key, fields)
	return ctx.Writer.WriteSimpleString("OK")
}

func cmdHExists(ctx *Context, args []string) error {
	if len(args) < 2 {
		return ctx.Writer.WriteError("wrong number of arguments for 'hexists'")
	}
	_, err := ctx.Backend.Get(ctx.Ctx, hashFieldKey(args[0], args[1]))
	if err != nil {
		return ctx.Writer.WriteInteger(0)
	}
	return ctx.Writer.WriteInteger(1)
}

func cmdHScan(ctx *Context, args []string) error {
	if len(args) < 2 {
		return ctx.Writer.WriteError("wrong number of arguments for 'hscan'")
	}
	key := args[0]
	cursor, _ := strconv.Atoi(args[1])
	pattern := "*"
	count := 10

	for i := 2; i+1 < len(args); i += 2 {
		switch strings.ToUpper(args[i]) {
		case "MATCH":
			pattern = args[i+1]
		case "COUNT":
			c, _ := strconv.Atoi(args[i+1])
			if c > 0 {
				count = c
			}
		}
	}

	fields, _ := getHashFields(ctx, key)
	var filtered []string
	for _, f := range fields {
		if matchRedisPattern(pattern, f) {
			filtered = append(filtered, f)
		}
	}
	sort.Strings(filtered)

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

	page := filtered[start:end]

	// Write [cursor, [field, value, field, value, ...]]
	if err := ctx.Writer.WriteArrayLen(2); err != nil {
		return err
	}
	if err := ctx.Writer.WriteBulkString([]byte(strconv.Itoa(nextCursor))); err != nil {
		return err
	}
	if err := ctx.Writer.WriteArrayLen(len(page) * 2); err != nil {
		return err
	}
	for _, f := range page {
		if err := ctx.Writer.WriteBulkString([]byte(f)); err != nil {
			return err
		}
		val, err := ctx.Backend.Get(ctx.Ctx, hashFieldKey(key, f))
		if err != nil {
			if err := ctx.Writer.WriteNil(); err != nil {
				return err
			}
		} else {
			if err := ctx.Writer.WriteBulkString(val); err != nil {
				return err
			}
		}
	}
	return nil
}
