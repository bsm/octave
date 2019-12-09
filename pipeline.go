package octave

import (
	"context"
	"fmt"
	"time"

	"github.com/bsm/accord"
	"github.com/bsm/bfs"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// Pipeline processes data by running parallel worker threads.
type Pipeline struct {
	src, dst   bfs.Bucket
	ownBuckets bool

	acc *accord.Client
	opt *Options

	sem    *semaphore.Weighted
	ctx    context.Context
	cancel context.CancelFunc
}

// Create creates a new Pipeline from URLs.
func Create(ctx context.Context, srcURL, dstURL string, acc *accord.Client, opt *Options) (*Pipeline, error) {
	src, err := bfs.Connect(ctx, srcURL)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to %s: %w", srcURL, err)
	}

	dst, err := bfs.Connect(ctx, dstURL)
	if err != nil {
		_ = src.Close()
		return nil, fmt.Errorf("unable to connect to %s: %w", dstURL, err)
	}

	pipe := New(ctx, src, dst, acc, opt)
	pipe.ownBuckets = true
	return pipe, nil
}

// New inits a new Pipeline.
func New(ctx context.Context, src, dst bfs.Bucket, acc *accord.Client, opt *Options) *Pipeline {
	opt = opt.norm()

	ctx, cancel := context.WithCancel(ctx)
	return &Pipeline{
		src:    src,
		dst:    dst,
		acc:    acc,
		opt:    opt,
		sem:    semaphore.NewWeighted(int64(opt.Concurrency)),
		ctx:    ctx,
		cancel: cancel,
	}
}

// Run starts the Pipeline and blocks until an error occurs or it is
// manually stopped by calling Close().
func (p *Pipeline) Run(fn ChannelFunc) error {
	pause := time.NewTimer(time.Second)
	defer pause.Stop()

	for {
		if err := p.opt.BeforeCycle(); err != nil {
			return fmt.Errorf("callback to BeforeCycle failed with: %w", err)
		}

		if err := p.run(fn); err != nil {
			return err
		}

		pause.Reset(p.opt.Pause)
		select {
		case <-p.ctx.Done():
			return nil
		case <-pause.C:
		}
	}
}

// Close stops the Pipeline and releases all resources.
func (p *Pipeline) Close() error {
	p.cancel()

	var err error
	if p.ownBuckets {
		if e := p.src.Close(); e != nil {
			err = e
		}
		if e := p.dst.Close(); e != nil {
			err = e
		}
	}
	return err
}

func (p *Pipeline) run(fn ChannelFunc) error {
	// work in parallel
	group, ctx := errgroup.WithContext(p.ctx)

	// create source iterator
	iter, err := p.src.Glob(ctx, p.opt.Glob)
	if err != nil {
		return fmt.Errorf("failed to glob bucket: %w", err)
	}
	defer iter.Close()

	// iterate over files
	for iter.Next() {
		name := iter.Name()

		// check if the file needs processing
		if ok, err := p.opt.ProcessFile(name); err != nil {
			return err
		} else if !ok {
			continue
		}

		// acquire a concurrency slot
		if err := p.sem.Acquire(ctx, 1); err == context.Canceled || err == context.DeadlineExceeded {
			break
		} else if err != nil {
			return fmt.Errorf("unabled to acquire worker slot: %w", err)
		}

		// launch a worker thread
		group.Go(func() error {
			defer p.sem.Release(1)

			if err := p.work(ctx, name, fn); err != nil {
				return fmt.Errorf("failed to process %q: %w", name, err)
			}
			return nil
		})
	}

	// wait for all workers to finish
	if err := group.Wait(); err != nil {
		return fmt.Errorf("worker thread exited with: %w", err)
	}

	// check iterator for errors
	if err := iter.Error(); err != nil {
		return fmt.Errorf("bucket iterator failed with: %w", err)
	}
	return nil
}

func (p *Pipeline) work(ctx context.Context, name string, fn ChannelFunc) error {
	// detect compression + coder
	compression := p.opt.findCompression(name)
	coder := p.opt.findCoder(name)
	if coder == nil {
		return errNoCoder
	}

	// acquire lock handle
	handle, err := p.acc.Acquire(ctx, name, nil)
	if err == accord.ErrAcquired || err == accord.ErrDone {
		return nil
	} else if err != nil {
		return err
	}
	defer handle.Discard()

	// open source
	rc, err := p.src.Open(ctx, name)
	if err != nil {
		return err
	}
	defer rc.Close()

	// open compression
	cc, err := compression.NewReader(rc)
	if err != nil {
		return err
	}
	defer cc.Close()

	// open decoder
	dec, err := coder.NewDecoder(cc)
	if err != nil {
		return err
	}
	defer dec.Close()

	// init emitter
	emt := &emitter{Decoder: dec, name: name}

	// open sink
	snk, err := newSink(ctx, p.opt)
	if err != nil {
		return err
	}
	defer snk.Discard()

	// process channel func
	if err := fn(emt, snk); err != nil {
		return err
	}

	// commit sink
	if err := snk.Commit(p.dst); err != nil {
		return err
	}

	// commit lock handle
	return handle.Done(ctx, nil)
}
