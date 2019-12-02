# Octave

[![Build Status](https://travis-ci.org/bsm/octave.png)](https://travis-ci.org/bsm/octave)
[![GoDoc](https://godoc.org/github.com/bsm/octave?status.png)](http://godoc.org/github.com/bsm/octave)
[![Go Report Card](https://goreportcard.com/badge/github.com/bsm/octave)](https://goreportcard.com/report/github.com/bsm/octave)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Octave is a Go library for file/bucket based event stream processing.

## Documentation

Check out the full API on [godoc.org](http://godoc.org/github.com/bsm/octave).

## Example

First, write/append your data to a log. You can index your logs:

```go
import (
	"context"
	"io"
	"path"
	"strings"

	"github.com/bsm/accord"
	"github.com/bsm/octave"
)

func main() {
	ctx := context.Background()

	// assume a mock type
	type mockType struct {
		Name	string
		Phone	string
		Country	string
	}

	// connect to accord
	acc, err := accord.DialClient(ctx, "10.0.0.1:8432", &accord.ClientOptions{Namespace: "/custom/namespace"})
	if err != nil {
		panic(err)
	}

	// initialize a pipeline
	pipe, err := octave.Create(ctx, "s3://source", "s3://target/to/dir", acc, &octave.Options{
		Glob:	"**/*.ndjson",
		ProcessFile: func(name string) (bool, error) {
			return strings.Contains(name, ".ndjson"), nil
		},
	})
	if err != nil {
		panic(err)
	}
	defer pipe.Close()

	// run the pipeline (blocking)
	err = pipe.Run(func(emt octave.Emitter, snk octave.Sink) error {
		for {
			// decode the record
			rec := new(mockType)
			if err := emt.Decode(rec); err == io.EOF {
				break
			} else if err != nil {
				return err
			}

			// get the source file name (without extension)
			name := path.Base(emt.Name())
			if pos := strings.IndexByte(name, '.'); pos > -1 {
				name = name[:pos]
			}

			// write to output
			if err := snk.Encode(name+"-"+rec.Country+".ndjson", rec); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
}
```
