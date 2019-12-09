package octave

import (
	"context"

	"github.com/bsm/bfs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Sink", func() {
	var subject *sink
	var ctx = context.Background()

	BeforeEach(func() {
		var err error
		subject, err = newSink(ctx, new(Options).norm())
		Expect(err).NotTo(HaveOccurred())

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
		bucket := bfs.NewInMem()
		Expect(bucket.ObjectSizes()).To(BeEmpty())
		Expect(subject.Commit(bucket)).To(Succeed())

		sizes := bucket.ObjectSizes()
		Expect(sizes).To(HaveLen(2))
		Expect(sizes).To(HaveKeyWithValue("file1.json", int64(250)))
		Expect(sizes).To(HaveKeyWithValue("file2.json.gz", BeNumerically("~", 52, 20)))
	})

	It("should discard", func() {
		bucket := bfs.NewInMem()
		Expect(bucket.ObjectSizes()).To(BeEmpty())
		Expect(subject.Discard()).To(Succeed())
		Expect(bucket.ObjectSizes()).To(BeEmpty())
	})
})
