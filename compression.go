package octave

import (
	"compress/gzip"
	"io"
)

// Compression represents the data compression codec.
type Compression interface {
	// NewReader wraps a reader.
	NewReader(io.Reader) (io.ReadCloser, error)
	// NewWriter wraps a writer.
	NewWriter(io.Writer) (io.WriteCloser, error)
}

type noCompression struct{}

func (noCompression) NewReader(r io.Reader) (io.ReadCloser, error) {
	return noCompressionWrapper{Reader: r}, nil
}

func (noCompression) NewWriter(w io.Writer) (io.WriteCloser, error) {
	return noCompressionWrapper{Writer: w}, nil
}

type noCompressionWrapper struct {
	io.Reader
	io.Writer
}

func (noCompressionWrapper) Close() error { return nil }

// GZipCompression supports gzip compression format.
var GZipCompression = gzipCompression{}

type gzipCompression struct{}

func (gzipCompression) NewReader(r io.Reader) (io.ReadCloser, error) {
	return gzip.NewReader(r)
}

func (gzipCompression) NewWriter(w io.Writer) (io.WriteCloser, error) {
	return gzip.NewWriter(w), nil
}
