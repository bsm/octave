package octave

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"path"
	"runtime"
	"time"
)

var (
	errNoEncoder = errors.New("unable to detect encoder")
	errNoDecoder = errors.New("unable to detect decoder")
)

// Options contains a list of options.
type Options struct {
	// Number of concurrent worker threads.
	// Default: number of CPUs
	Concurrency int

	// A custom temporary directory.
	// Default: os.TempDir()
	TempDir string

	// File glob pattern.
	// Default: "**"
	Glob string

	// NewDecoder wraps the reader and returns a decoder for the given file name.
	// Default: json.NewDecoder(reader)
	NewDecoder func(name string, reader io.Reader) (Decoder, error)

	// NewEncoder wraps the writer and returns an encoder for the given file name.
	// Default: json.NewEncoder(writer)
	NewEncoder func(name string, writer io.Writer) (Encoder, error)

	// NewCompressionReader wraps the reader and returns an io.ReadCloser.
	// It may return nil to disable decompression and read the plain input.
	// Default: gzip.NewReader(reader) (if name's extension is .gz)
	NewCompressionReader func(name string, reader io.Reader) (io.ReadCloser, error)

	// NewCompressionWriter wraps the writer and returns an io.WriteCloser.
	// It may return nil to disable compression and write the plain output.
	// Default: gzip.NewWriter(writer) (if name's extension is .gz)
	NewCompressionWriter func(name string, writer io.Writer) (io.WriteCloser, error)

	// Pause between cycles. This is to prevernt the Pipeline
	// from spinning and wasting resources on empty or processed
	// buckets.
	// Default: 5s.
	Pause time.Duration

	// BeforeCycle is a callback which is triggered before each cycles.
	BeforeCycle func() error

	// ProcessFile callback is triggered before processing to determine
	// if a file should be processed or skipped. Must return true to proceed.
	ProcessFile func(name string) (bool, error)
}

func (o *Options) newDecoder(name string, r io.Reader) (Decoder, error) {
	if o.NewDecoder == nil {
		return jsonWrapper{Decoder: json.NewDecoder(r)}, nil
	}

	dec, err := o.NewDecoder(name, r)
	if err != nil {
		return nil, err
	} else if dec == nil {
		return nil, errNoDecoder
	}
	return dec, nil
}

func (o *Options) newEncoder(name string, w io.Writer) (Encoder, error) {
	if o.NewEncoder == nil {
		return jsonWrapper{Encoder: json.NewEncoder(w)}, nil
	}

	enc, err := o.NewEncoder(name, w)
	if err != nil {
		return nil, err
	} else if enc == nil {
		return nil, errNoEncoder
	}
	return enc, nil
}

func (o *Options) newCompressionReader(name string, r io.Reader) (rc io.ReadCloser, err error) {
	if o.NewCompressionReader == nil {
		if path.Ext(path.Base(name)) == ".gz" {
			rc, err = gzip.NewReader(r)
		}
	} else {
		rc, err = o.NewCompressionReader(name, r)
	}

	if rc != nil || err != nil {
		return rc, err
	}
	return noCompression{Reader: r}, nil
}

func (o *Options) newCompressionWriter(name string, w io.Writer) (wc io.WriteCloser, err error) {
	if o.NewCompressionWriter == nil {
		if path.Ext(path.Base(name)) == ".gz" {
			wc = gzip.NewWriter(w)
		}
	} else {
		wc, err = o.NewCompressionWriter(name, w)
	}

	if wc != nil || err != nil {
		return wc, err
	}
	return noCompression{Writer: w}, nil
}

func (o *Options) norm() *Options {
	var o2 Options
	if o != nil {
		o2 = *o
	}

	if o2.Concurrency <= 0 {
		o2.Concurrency = runtime.NumCPU()
	}

	if o2.Glob == "" {
		o2.Glob = "**"
	}

	if o2.Pause <= 0 {
		o2.Pause = 5 * time.Second
	}

	if o2.BeforeCycle == nil {
		o2.BeforeCycle = func() error { return nil }
	}
	if o2.ProcessFile == nil {
		o2.ProcessFile = func(_ string) (bool, error) { return true, nil }
	}

	return &o2
}
