package command

import (
	"fmt"
	"runtime"
	"strings"
	"time"
)

var startTime = time.Now()

func registerServerCommands(r *Registry) {
	r.Register("INFO", cmdInfo, CommandInfo{Name: "INFO", Arity: -1})
	r.Register("COMMAND", cmdCommand, CommandInfo{Name: "COMMAND", Arity: -1})
}

func cmdInfo(ctx *Context, args []string) error {
	uptime := int64(time.Since(startTime).Seconds())
	connected := "1"
	if !ctx.Backend.IsConnected() {
		connected = "0"
	}

	info := fmt.Sprintf(
		"# Server\r\n"+
			"nats_jetstream_kv_resp_version:0.1.0\r\n"+
			"go_version:%s\r\n"+
			"os:%s\r\n"+
			"arch:%s\r\n"+
			"uptime_in_seconds:%d\r\n"+
			"nats_connected:%s\r\n"+
			"active_bucket:%s\r\n"+
			"\r\n"+
			"# Keyspace\r\n",
		runtime.Version(),
		runtime.GOOS,
		runtime.GOARCH,
		uptime,
		connected,
		ctx.Backend.ActiveBucket(),
	)

	// Add key count
	count, err := ctx.Backend.BucketKeyCount(ctx.Ctx)
	if err == nil {
		info += fmt.Sprintf("db0:keys=%d,expires=0\r\n", count)
	}

	return ctx.Writer.WriteBulkString([]byte(info))
}

func cmdCommand(ctx *Context, args []string) error {
	if len(args) > 0 {
		switch strings.ToUpper(args[0]) {
		case "COUNT":
			return ctx.Writer.WriteInteger(int64(ctx.ConnState.SelectedDB))
		case "DOCS":
			return ctx.Writer.WriteArrayLen(0)
		}
	}
	return ctx.Writer.WriteSimpleString("OK")
}
