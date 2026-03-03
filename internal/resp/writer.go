package resp

import (
	"bufio"
	"fmt"
	"io"
)

// Writer writes RESP protocol responses.
type Writer struct {
	w *bufio.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{w: bufio.NewWriterSize(w, 64*1024)}
}

func (w *Writer) WriteSimpleString(s string) error {
	_, err := fmt.Fprintf(w.w, "+%s\r\n", s)
	return err
}

func (w *Writer) WriteError(msg string) error {
	_, err := fmt.Fprintf(w.w, "-ERR %s\r\n", msg)
	return err
}

func (w *Writer) WriteErrorType(errType, msg string) error {
	_, err := fmt.Fprintf(w.w, "-%s %s\r\n", errType, msg)
	return err
}

func (w *Writer) WriteInteger(n int64) error {
	_, err := fmt.Fprintf(w.w, ":%d\r\n", n)
	return err
}

func (w *Writer) WriteBulkString(data []byte) error {
	if _, err := fmt.Fprintf(w.w, "$%d\r\n", len(data)); err != nil {
		return err
	}
	if _, err := w.w.Write(data); err != nil {
		return err
	}
	_, err := w.w.WriteString("\r\n")
	return err
}

func (w *Writer) WriteNil() error {
	_, err := w.w.WriteString("$-1\r\n")
	return err
}

func (w *Writer) WriteNilArray() error {
	_, err := w.w.WriteString("*-1\r\n")
	return err
}

func (w *Writer) WriteArrayLen(n int) error {
	_, err := fmt.Fprintf(w.w, "*%d\r\n", n)
	return err
}

func (w *Writer) Flush() error {
	return w.w.Flush()
}
