package octave

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/bsm/bfs"
)

var errDiscarded = errors.New("sink is already discarded")

type sink struct {
	dir    string
	opt    *Options
	files  map[string]*sinkFile
	buf    []byte
	ctx    context.Context
	cancel context.CancelFunc
}

func newSink(ctx context.Context, o *Options) (*sink, error) {
	dir, err := ioutil.TempDir(o.TempDir, "octave-")
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	return &sink{
		dir:    dir,
		opt:    o,
		files:  make(map[string]*sinkFile),
		ctx:    ctx,
		cancel: cancel,
	}, nil
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
	return s.Commit(nil)
}

// Commit commits all stashed messages.
func (s *sink) Commit(bucket bfs.Bucket) (err error) {
	defer os.RemoveAll(s.dir)
	defer func() { s.files = make(map[string]*sinkFile) }()

	for _, file := range s.files {
		if e := file.Close(); e != nil && e != context.Canceled {
			err = e // close all files!
		}
	}

	if bucket == nil || s.ctx.Err() != nil {
		return
	}

	for name := range s.files {
		if err := s.sendFile(bucket, name); err != nil {
			return err // exit on first error!
		}
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

	wc, err := os.Create(filepath.Join(s.dir, name))
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

func (s *sink) sendFile(bucket bfs.Bucket, name string) error {
	w, err := bucket.Create(s.ctx, name, nil)
	if err != nil {
		return err
	}
	defer w.Close()

	r, err := os.Open(filepath.Join(s.dir, name))
	if err != nil {
		return err
	}
	defer r.Close()

	if len(s.buf) == 0 {
		s.buf = make([]byte, 32*1024)
	}

	if _, err := io.CopyBuffer(w, r, s.buf); err != nil {
		return err
	}

	return w.Close()
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
