package octave

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/bsm/bfs"
)

var errDiscarded = errors.New("sink is already discarded")

type sink struct {
	bucket bfs.Bucket
	opt    *Options
	files  map[string]*sinkFile
	ctx    context.Context
	cancel context.CancelFunc
}

func newSink(ctx context.Context, b bfs.Bucket, o *Options) *sink {
	ctx, cancel := context.WithCancel(ctx)
	return &sink{
		bucket: b,
		files:  make(map[string]*sinkFile),
		opt:    o,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Encode appends a message to a file.
func (s *sink) Encode(name string, msg interface{}) error {
	file, err := s.fetch(name)
	if err != nil {
		return fmt.Errorf("unable to open encoder %q: %w", name, err)
	}

	if err := file.Encode(msg); err != nil {
		return fmt.Errorf("unable to encode message to %q: %w", name, err)
	}
	return nil
}

// Discard discards all stashed messages.
func (s *sink) Discard() error {
	s.cancel()
	return s.Commit()
}

// Commit commits all stashed messages.
func (s *sink) Commit() (err error) {
	for name, file := range s.files {
		if e := file.Close(); e != nil && e != context.Canceled {
			err = e
		}
		delete(s.files, name)
	}
	return
}

func (s *sink) fetch(name string) (*sinkFile, error) {
	if file, ok := s.files[name]; ok {
		return file, nil
	}

	compression := s.opt.findCompression(name)
	coder := s.opt.findCoder(name)
	if coder == nil {
		return nil, errNoCoder
	}

	wc, err := s.bucket.Create(s.ctx, name, nil)
	if err != nil {
		return nil, err
	}

	cc, err := compression.NewWriter(wc)
	if err != nil {
		_ = wc.Close()
		return nil, err
	}

	enc, err := coder.NewEncoder(cc)
	if err != nil {
		_ = wc.Close()
		_ = cc.Close()
		return nil, err
	}

	file := &sinkFile{Encoder: enc, c: cc, w: wc}
	s.files[name] = file
	return file, nil
}

type sinkFile struct {
	Encoder
	c, w io.WriteCloser
}

func (s *sinkFile) Close() (err error) {
	if e := s.Encoder.Close(); e != nil {
		err = e
	}
	if e := s.c.Close(); e != nil {
		err = e
	}
	if e := s.w.Close(); e != nil {
		err = e
	}
	return
}
