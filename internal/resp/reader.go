package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Reader parses RESP protocol commands from a buffered reader.
type Reader struct {
	r          *bufio.Reader
	maxArgs    int
	maxBulkLen int64
}

func NewReader(r io.Reader, maxArgs int, maxBulkLen int64) *Reader {
	if maxArgs <= 0 {
		maxArgs = 1024
	}
	if maxBulkLen <= 0 {
		maxBulkLen = 512 * 1024 * 1024
	}
	return &Reader{r: bufio.NewReaderSize(r, 64*1024), maxArgs: maxArgs, maxBulkLen: maxBulkLen}
}

// ReadCommand parses a single RESP command (inline or multi-bulk).
func (r *Reader) ReadCommand() ([]string, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, nil
	}
	// Inline command
	if line[0] != '*' {
		return strings.Fields(line), nil
	}
	// Multi-bulk
	n, err := strconv.Atoi(line[1:])
	if err != nil || n < 0 {
		return nil, fmt.Errorf("invalid RESP array length: %q", line)
	}
	if n > r.maxArgs {
		return nil, fmt.Errorf("too many arguments: %d (max %d)", n, r.maxArgs)
	}
	args := make([]string, n)
	for i := 0; i < n; i++ {
		s, err := r.readBulkString()
		if err != nil {
			return nil, err
		}
		args[i] = s
	}
	return args, nil
}

func (r *Reader) readLine() (string, error) {
	line, err := r.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

func (r *Reader) readBulkString() (string, error) {
	line, err := r.readLine()
	if err != nil {
		return "", err
	}
	if len(line) == 0 || line[0] != '$' {
		return "", fmt.Errorf("expected bulk string header, got %q", line)
	}
	n, err := strconv.ParseInt(line[1:], 10, 64)
	if err != nil || n < -1 {
		return "", fmt.Errorf("invalid bulk string length: %q", line)
	}
	if n == -1 {
		return "", nil // nil bulk string
	}
	if n > r.maxBulkLen {
		return "", fmt.Errorf("bulk string too large: %d (max %d)", n, r.maxBulkLen)
	}
	buf := make([]byte, n+2) // +2 for trailing \r\n
	if _, err := io.ReadFull(r.r, buf); err != nil {
		return "", fmt.Errorf("reading bulk string data: %w", err)
	}
	return string(buf[:n]), nil
}
