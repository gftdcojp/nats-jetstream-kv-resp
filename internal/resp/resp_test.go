package resp_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/gftdcojp/nats-jetstream-kv-resp/internal/resp"
)

func TestReader_InlineCommand(t *testing.T) {
	input := "PING\r\n"
	r := resp.NewReader(strings.NewReader(input), 128, 1<<20)
	args, err := r.ReadCommand()
	if err != nil {
		t.Fatalf("ReadCommand: %v", err)
	}
	if len(args) != 1 || args[0] != "PING" {
		t.Fatalf("expected [PING], got %v", args)
	}
}

func TestReader_MultiBulk(t *testing.T) {
	input := "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
	r := resp.NewReader(strings.NewReader(input), 128, 1<<20)
	args, err := r.ReadCommand()
	if err != nil {
		t.Fatalf("ReadCommand: %v", err)
	}
	if len(args) != 3 || args[0] != "SET" || args[1] != "foo" || args[2] != "bar" {
		t.Fatalf("expected [SET foo bar], got %v", args)
	}
}

func TestReader_InlineMultiWord(t *testing.T) {
	input := "SET key value\r\n"
	r := resp.NewReader(strings.NewReader(input), 128, 1<<20)
	args, err := r.ReadCommand()
	if err != nil {
		t.Fatalf("ReadCommand: %v", err)
	}
	if len(args) != 3 || args[0] != "SET" || args[1] != "key" || args[2] != "value" {
		t.Fatalf("expected [SET key value], got %v", args)
	}
}

func TestWriter_SimpleString(t *testing.T) {
	var buf bytes.Buffer
	w := resp.NewWriter(&buf)
	if err := w.WriteSimpleString("OK"); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if buf.String() != "+OK\r\n" {
		t.Fatalf("expected '+OK\\r\\n', got %q", buf.String())
	}
}

func TestWriter_BulkString(t *testing.T) {
	var buf bytes.Buffer
	w := resp.NewWriter(&buf)
	if err := w.WriteBulkString([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if buf.String() != "$5\r\nhello\r\n" {
		t.Fatalf("expected '$5\\r\\nhello\\r\\n', got %q", buf.String())
	}
}

func TestWriter_Nil(t *testing.T) {
	var buf bytes.Buffer
	w := resp.NewWriter(&buf)
	if err := w.WriteNil(); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if buf.String() != "$-1\r\n" {
		t.Fatalf("expected '$-1\\r\\n', got %q", buf.String())
	}
}

func TestWriter_Integer(t *testing.T) {
	var buf bytes.Buffer
	w := resp.NewWriter(&buf)
	if err := w.WriteInteger(42); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if buf.String() != ":42\r\n" {
		t.Fatalf("expected ':42\\r\\n', got %q", buf.String())
	}
}

func TestWriter_Error(t *testing.T) {
	var buf bytes.Buffer
	w := resp.NewWriter(&buf)
	if err := w.WriteError("test error"); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if buf.String() != "-ERR test error\r\n" {
		t.Fatalf("expected '-ERR test error\\r\\n', got %q", buf.String())
	}
}

func TestWriter_ArrayLen(t *testing.T) {
	var buf bytes.Buffer
	w := resp.NewWriter(&buf)
	if err := w.WriteArrayLen(3); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if buf.String() != "*3\r\n" {
		t.Fatalf("expected '*3\\r\\n', got %q", buf.String())
	}
}
