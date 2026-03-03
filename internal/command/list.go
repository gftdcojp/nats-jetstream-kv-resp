package command

import (
	"encoding/json"
	"strconv"

	"github.com/gftdcojp/nats-jetstream-kv-resp/internal/store"
)

func registerListCommands(r *Registry) {
	r.Register("LPUSH", cmdLPush, CommandInfo{Name: "LPUSH", Arity: -3})
	r.Register("RPUSH", cmdRPush, CommandInfo{Name: "RPUSH", Arity: -3})
	r.Register("LPOP", cmdLPop, CommandInfo{Name: "LPOP", Arity: -2})
	r.Register("RPOP", cmdRPop, CommandInfo{Name: "RPOP", Arity: -2})
	r.Register("LRANGE", cmdLRange, CommandInfo{Name: "LRANGE", Arity: 4})
	r.Register("LLEN", cmdLLen, CommandInfo{Name: "LLEN", Arity: 2})
	r.Register("LINDEX", cmdLIndex, CommandInfo{Name: "LINDEX", Arity: 3})
	r.Register("LSET", cmdLSet, CommandInfo{Name: "LSET", Arity: 4})
}

func listKey(key string) string { return "_l." + key }

func getList(ctx *Context, key string) ([]string, error) {
	data, err := ctx.Backend.Get(ctx.Ctx, listKey(key))
	if err != nil {
		if err == store.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}
	var list []string
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, err
	}
	return list, nil
}

func saveList(ctx *Context, key string, list []string) error {
	if len(list) == 0 {
		return ctx.Backend.Delete(ctx.Ctx, listKey(key))
	}
	data, _ := json.Marshal(list)
	_, err := ctx.Backend.Put(ctx.Ctx, listKey(key), data)
	return err
}

func cmdLPush(ctx *Context, args []string) error {
	if len(args) < 2 {
		return ctx.Writer.WriteError("wrong number of arguments for 'lpush'")
	}
	list, err := getList(ctx, args[0])
	if err != nil {
		return ctx.Writer.WriteError(err.Error())
	}
	// Prepend in reverse order (Redis behavior: LPUSH key a b c → [c, b, a, ...])
	newElems := make([]string, len(args)-1)
	for i, v := range args[1:] {
		newElems[len(args)-2-i] = v
	}
	list = append(newElems, list...)
	if err := saveList(ctx, args[0], list); err != nil {
		return ctx.Writer.WriteError(err.Error())
	}
	return ctx.Writer.WriteInteger(int64(len(list)))
}

func cmdRPush(ctx *Context, args []string) error {
	if len(args) < 2 {
		return ctx.Writer.WriteError("wrong number of arguments for 'rpush'")
	}
	list, err := getList(ctx, args[0])
	if err != nil {
		return ctx.Writer.WriteError(err.Error())
	}
	list = append(list, args[1:]...)
	if err := saveList(ctx, args[0], list); err != nil {
		return ctx.Writer.WriteError(err.Error())
	}
	return ctx.Writer.WriteInteger(int64(len(list)))
}

func cmdLPop(ctx *Context, args []string) error {
	if len(args) < 1 {
		return ctx.Writer.WriteError("wrong number of arguments for 'lpop'")
	}
	list, err := getList(ctx, args[0])
	if err != nil {
		return ctx.Writer.WriteError(err.Error())
	}
	if len(list) == 0 {
		return ctx.Writer.WriteNil()
	}

	count := 1
	if len(args) > 1 {
		c, cerr := strconv.Atoi(args[1])
		if cerr != nil || c < 0 {
			return ctx.Writer.WriteError("value is not an integer or out of range")
		}
		count = c
	}

	if count > len(list) {
		count = len(list)
	}
	popped := list[:count]
	list = list[count:]
	if err := saveList(ctx, args[0], list); err != nil {
		return ctx.Writer.WriteError(err.Error())
	}

	if len(args) > 1 {
		// Return array when count specified
		if err := ctx.Writer.WriteArrayLen(len(popped)); err != nil {
			return err
		}
		for _, v := range popped {
			if err := ctx.Writer.WriteBulkString([]byte(v)); err != nil {
				return err
			}
		}
		return nil
	}
	return ctx.Writer.WriteBulkString([]byte(popped[0]))
}

func cmdRPop(ctx *Context, args []string) error {
	if len(args) < 1 {
		return ctx.Writer.WriteError("wrong number of arguments for 'rpop'")
	}
	list, err := getList(ctx, args[0])
	if err != nil {
		return ctx.Writer.WriteError(err.Error())
	}
	if len(list) == 0 {
		return ctx.Writer.WriteNil()
	}

	count := 1
	if len(args) > 1 {
		c, cerr := strconv.Atoi(args[1])
		if cerr != nil || c < 0 {
			return ctx.Writer.WriteError("value is not an integer or out of range")
		}
		count = c
	}

	if count > len(list) {
		count = len(list)
	}
	popped := list[len(list)-count:]
	list = list[:len(list)-count]
	if err := saveList(ctx, args[0], list); err != nil {
		return ctx.Writer.WriteError(err.Error())
	}

	if len(args) > 1 {
		if err := ctx.Writer.WriteArrayLen(len(popped)); err != nil {
			return err
		}
		// Redis returns in pop order (rightmost first)
		for i := len(popped) - 1; i >= 0; i-- {
			if err := ctx.Writer.WriteBulkString([]byte(popped[i])); err != nil {
				return err
			}
		}
		return nil
	}
	return ctx.Writer.WriteBulkString([]byte(popped[len(popped)-1]))
}

func cmdLRange(ctx *Context, args []string) error {
	if len(args) < 3 {
		return ctx.Writer.WriteError("wrong number of arguments for 'lrange'")
	}
	list, err := getList(ctx, args[0])
	if err != nil {
		return ctx.Writer.WriteError(err.Error())
	}
	start, _ := strconv.Atoi(args[1])
	stop, _ := strconv.Atoi(args[2])

	n := len(list)
	if start < 0 {
		start = n + start
	}
	if stop < 0 {
		stop = n + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}

	if start > stop || start >= n {
		return ctx.Writer.WriteArrayLen(0)
	}

	result := list[start : stop+1]
	if err := ctx.Writer.WriteArrayLen(len(result)); err != nil {
		return err
	}
	for _, v := range result {
		if err := ctx.Writer.WriteBulkString([]byte(v)); err != nil {
			return err
		}
	}
	return nil
}

func cmdLLen(ctx *Context, args []string) error {
	if len(args) < 1 {
		return ctx.Writer.WriteError("wrong number of arguments for 'llen'")
	}
	list, err := getList(ctx, args[0])
	if err != nil {
		return ctx.Writer.WriteError(err.Error())
	}
	return ctx.Writer.WriteInteger(int64(len(list)))
}

func cmdLIndex(ctx *Context, args []string) error {
	if len(args) < 2 {
		return ctx.Writer.WriteError("wrong number of arguments for 'lindex'")
	}
	list, err := getList(ctx, args[0])
	if err != nil {
		return ctx.Writer.WriteError(err.Error())
	}
	idx, _ := strconv.Atoi(args[1])
	if idx < 0 {
		idx = len(list) + idx
	}
	if idx < 0 || idx >= len(list) {
		return ctx.Writer.WriteNil()
	}
	return ctx.Writer.WriteBulkString([]byte(list[idx]))
}

func cmdLSet(ctx *Context, args []string) error {
	if len(args) < 3 {
		return ctx.Writer.WriteError("wrong number of arguments for 'lset'")
	}
	list, err := getList(ctx, args[0])
	if err != nil {
		return ctx.Writer.WriteError(err.Error())
	}
	if len(list) == 0 {
		return ctx.Writer.WriteError("no such key")
	}
	idx, _ := strconv.Atoi(args[1])
	if idx < 0 {
		idx = len(list) + idx
	}
	if idx < 0 || idx >= len(list) {
		return ctx.Writer.WriteError("index out of range")
	}
	list[idx] = args[2]
	if err := saveList(ctx, args[0], list); err != nil {
		return ctx.Writer.WriteError(err.Error())
	}
	return ctx.Writer.WriteSimpleString("OK")
}
