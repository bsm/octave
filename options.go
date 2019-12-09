package octave

import (
	"path"
	"runtime"
	"strings"
	"time"
)

// Options contains a list of options.
type Options struct {
	// Number of concurrent worker threads.
	// Default: 4 * number of CPUs
	Concurrency int

	// A custom temporary directory.
	// Default: os.TempDir()
	TempDir string

	// File glob pattern.
	// Default: "**"
	Glob string

	// Coders defines the available coder formats.
	// Default: { ".json": JSONCoder }
	Coders map[string]Coder

	// Compressions defines the compression formats.
	// Default: { ".gz": GZipCompression }
	Compressions map[string]Compression

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

func (o *Options) findCoder(name string) Coder {
	ext := path.Ext(path.Base(name))
	if ext == "" {
		return nil
	} else if c, ok := o.Coders[ext]; ok {
		return c
	}
	return o.findCoder(strings.TrimSuffix(name, ext))
}

func (o *Options) findCompression(name string) Compression {
	ext := path.Ext(path.Base(name))
	if ext == "" {
		return noCompression{}
	} else if c, ok := o.Compressions[ext]; ok {
		return c
	}
	return o.findCompression(strings.TrimSuffix(name, ext))
}

func (o *Options) norm() *Options {
	var o2 Options
	if o != nil {
		o2 = *o
	}

	if o2.Concurrency <= 0 {
		o2.Concurrency = 4 * runtime.NumCPU()
	}

	if o2.Glob == "" {
		o2.Glob = "**"
	}

	if len(o2.Coders) == 0 {
		o2.Coders = map[string]Coder{
			".json":   JSONCoder,
			".ndjson": JSONCoder,
		}
	}

	if len(o2.Compressions) == 0 {
		o2.Compressions = map[string]Compression{".gz": GZipCompression}
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
