package octave

import (
	"context"
	"io"
	"path"
	"strings"

	"github.com/bsm/accord"
	"github.com/bsm/accord/backend"
	"github.com/bsm/accord/backend/direct"
	"github.com/bsm/accord/backend/mock"
	"github.com/bsm/bfs"
	_ "github.com/bsm/bfs/bfsfs" // using file:// in tests
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Pipeline", func() {
	var subject *Pipeline

	var (
		src   bfs.Bucket
		dst   *bfs.InMem
		acc   *accord.Client
		accBE *mock.Backend
		ctx   = context.Background()
	)

	chanFunc := func(emt Emitter, snk Sink) error {
		for {
			msg := new(mockType)
			if err := emt.Decode(&msg); err == io.EOF {
				break
			} else if err != nil {
				return err
			}

			if msg.N != 5 {
				prefix := path.Base(emt.Name())
				if pos := strings.IndexByte(prefix, '.'); pos > -1 {
					prefix = prefix[:pos]
				}
				if err := snk.Encode(prefix+msg.S+".ndjson", msg); err != nil {
					return err
				}
			}
		}
		return nil
	}

	BeforeEach(func() {
		var err error
		src, err = bfs.Connect(ctx, "file://./testdata")
		Expect(err).NotTo(HaveOccurred())

		dst = bfs.NewInMem()

		accBE = mock.New()
		acc, err = accord.RPCClient(ctx, direct.Connect(accBE), nil)
		Expect(err).NotTo(HaveOccurred())

		subject = New(ctx, src, dst, acc, &Options{
			ProcessFile: func(name string) (bool, error) {
				return strings.HasPrefix(name, "data-"), nil
			},
		})
	})

	AfterEach(func() {
		Expect(subject.Close()).To(Succeed())
		Expect(acc.Close()).To(Succeed())
		Expect(dst.Close()).To(Succeed())
		Expect(src.Close()).To(Succeed())
	})

	It("should run", func() {
		Expect(subject.run(chanFunc)).To(Succeed())
		Expect(dst.ObjectSizes()).To(Equal(map[string]int64{
			"data-1a.ndjson": 168,
			"data-1b.ndjson": 120,
			"data-1c.ndjson": 96,
			"data-1d.ndjson": 96,
			"data-1e.ndjson": 72,
			"data-1f.ndjson": 24,
			"data-1g.ndjson": 24,
			"data-2a.ndjson": 125,
			"data-2b.ndjson": 150,
			"data-2c.ndjson": 125,
			"data-2d.ndjson": 125,
			"data-2e.ndjson": 100,
		}))

		var handles []string
		Expect(accBE.List(ctx, nil, func(h *backend.HandleData) error {
			handles = append(handles, h.Name)
			return nil
		})).To(Succeed())
		Expect(handles).To(ConsistOf(
			"data-1.ndjson",
			"data-2.json.gz",
		))
	})
})
