package octave

import "io"

// Decoder methods
type Decoder interface {
	// Decode decodes the next message into an interface.
	// It returns io.EOF once the end of the stream is reached.
	Decode(v interface{}) error

	io.Closer
}

// Encoder methods
type Encoder interface {
	// Encode encodes the value.
	Encode(v interface{}) error

	io.Closer
}

// Emitter is a minimal decoder.
type Emitter interface {
	// Name returns the name of the file the data is emitted from.
	Name() string

	// Decode decodes the next message into an interface.
	// It returns io.EOF once the end of the stream is reached.
	Decode(interface{}) error
}

// Sink is a multi-file encoder.
type Sink interface {
	// Encode encodes the value to the given file name.
	Encode(name string, v interface{}) error
}

// ChannelFunc connects emitters to sinks.
type ChannelFunc func(Emitter, Sink) error

// --------------------------------------------------------------------

type emitter struct {
	Decoder
	name string
}

func (e *emitter) Name() string { return e.name }
