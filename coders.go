package octave

import (
	"encoding/json"
)

type jsonWrapper struct {
	*json.Encoder
	*json.Decoder
}

func (jsonWrapper) Close() error { return nil }
