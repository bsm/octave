package octave

import (
	"context"

	"github.com/bsm/bfs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Sink", func() {
	var subject *sink

	var bucket *bfs.InMem
	var ctx = context.Background()

	BeforeEach(func() {
		bucket = bfs.NewInMem()
		subject = newSink(ctx, bucket, new(Options).norm())

		for i := 0; i < 10; i++ {
			Expect(subject.Encode("file1.json", &mockType{S: "x", N: 1, F: 2.35})).To(Succeed())
		}
		for i := 0; i < 10; i++ {
			Expect(subject.Encode("file2.json.gz", &mockType{S: "y", N: 2, F: 3.72})).To(Succeed())
		}
	})

	AfterEach(func() {
		Expect(subject.Discard()).To(Succeed())
	})

	It("should append/commit", func() {
		Expect(bucket.ObjectSizes()).To(BeEmpty())
		Expect(subject.Commit()).To(Succeed())

		sizes := bucket.ObjectSizes()
		Expect(sizes).To(HaveLen(2))
		Expect(sizes).To(HaveKeyWithValue("file1.json", int64(250)))
		Expect(sizes).To(HaveKeyWithValue("file2.json.gz", BeNumerically("~", 52, 20)))
	})

	It("should discard", func() {
		Expect(bucket.ObjectSizes()).To(BeEmpty())
		Expect(subject.Discard()).To(Succeed())
		Expect(bucket.ObjectSizes()).To(BeEmpty())
	})
})
