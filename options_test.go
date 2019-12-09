package octave

import (
	"bytes"
	"compress/gzip"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Options", func() {
	var subject *Options

	BeforeEach(func() {
		subject = subject.norm()
	})

	It("should init", func() {
		Expect(subject).NotTo(BeNil())
		Expect(subject.Concurrency).To(BeNumerically(">", 0))
		Expect(subject.Pause).To(Equal(5 * time.Second))
		Expect(subject.BeforeCycle()).To(Succeed())
		Expect(subject.ProcessFile("any.ndjson")).To(BeTrue())
	})

	It("should have a default decoder", func() {
		dec, err := subject.newDecoder("test.json", bytes.NewReader(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(dec).To(BeAssignableToTypeOf(jsonWrapper{}))
		Expect(dec.Close()).To(Succeed())
	})

	It("should have a default encoder", func() {
		enc, err := subject.newEncoder("test.json", &bytes.Buffer{})
		Expect(err).NotTo(HaveOccurred())
		Expect(enc).To(BeAssignableToTypeOf(jsonWrapper{}))
		Expect(enc.Close()).To(Succeed())
	})

	It("should read compressed", func() {
		rc, err := subject.newCompressionReader("test.json", bytes.NewReader(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(rc).To(BeAssignableToTypeOf(noCompression{}))
		Expect(rc.Close()).To(Succeed())

		blankGzip := []byte{31, 139, 8, 0, 0, 0, 0, 0, 0, 255, 1, 0, 0, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0}
		rc, err = subject.newCompressionReader("test.json.gz", bytes.NewReader(blankGzip))
		Expect(err).NotTo(HaveOccurred())
		Expect(rc).To(BeAssignableToTypeOf(&gzip.Reader{}))
		Expect(rc.Close()).To(Succeed())
	})

	It("should write compressed", func() {
		wc, err := subject.newCompressionWriter("test.json", &bytes.Buffer{})
		Expect(err).NotTo(HaveOccurred())
		Expect(wc).To(BeAssignableToTypeOf(noCompression{}))
		Expect(wc.Close()).To(Succeed())

		wc, err = subject.newCompressionWriter("test.json.gz", &bytes.Buffer{})
		Expect(err).NotTo(HaveOccurred())
		Expect(wc).To(BeAssignableToTypeOf(&gzip.Writer{}))
		Expect(wc.Close()).To(Succeed())
	})
})
