package octave

import (
	"io"
)

type noCompression struct {
	io.Reader
	io.Writer
}

func (noCompression) Close() error { return nil }
