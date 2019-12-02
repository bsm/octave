package octave

import (
	"encoding/json"
	"errors"
	"io"
)

var errNoCoder = errors.New("unable to detect coder")

// Coder represents the data coder.
type Coder interface {
	// NewDecoder wraps a decoder around a reader.
	NewDecoder(io.Reader) (Decoder, error)
	// NewEncoder wraps an encoder around a writer.
	NewEncoder(io.Writer) (Encoder, error)
}

// JSONCoder implements Coder.
var JSONCoder Coder = jsonCoder{}

type jsonCoder struct{}

// NewDecoder implements Coder.
func (jsonCoder) NewDecoder(r io.Reader) (Decoder, error) {
	return jsonDecoderWrapper{Decoder: json.NewDecoder(r)}, nil
}

// NewEncoder implements Coder.
func (jsonCoder) NewEncoder(w io.Writer) (Encoder, error) {
	return jsonEncoderWrapper{Encoder: json.NewEncoder(w)}, nil
}

type jsonDecoderWrapper struct{ *json.Decoder }

func (jsonDecoderWrapper) Close() error { return nil }

type jsonEncoderWrapper struct{ *json.Encoder }

func (jsonEncoderWrapper) Close() error { return nil }
