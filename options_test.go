package octave

import (
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
		Expect(subject.Coders).To(HaveLen(2))
		Expect(subject.Compressions).To(HaveLen(1))
		Expect(subject.Pause).To(Equal(5 * time.Second))
		Expect(subject.BeforeCycle()).To(Succeed())
		Expect(subject.ProcessFile("any.ndjson")).To(BeTrue())
	})

	It("should detect coder", func() {
		Expect(subject.findCoder("test.json")).To(Equal(JSONCoder))
		Expect(subject.findCoder("/path/to/test.ndjson")).To(Equal(JSONCoder))
		Expect(subject.findCoder("test.json.gz")).To(Equal(JSONCoder))
		Expect(subject.findCoder("prefix/test.ndjson.schema.gz")).To(Equal(JSONCoder))

		Expect(subject.findCoder("test.txt")).To(BeNil())
		Expect(subject.findCoder("test")).To(BeNil())
		Expect(subject.findCoder("json")).To(BeNil())
		Expect(subject.findCoder("/path/to/")).To(BeNil())
	})

	It("should detect compression", func() {
		Expect(subject.findCompression("test.json")).To(Equal(noCompression{}))
		Expect(subject.findCompression("test.json.gz")).To(Equal(GZipCompression))
		Expect(subject.findCompression("test")).To(Equal(noCompression{}))
		Expect(subject.findCompression("test.ndjson.gz.suffix")).To(Equal(GZipCompression))
	})
})
