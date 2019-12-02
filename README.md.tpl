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

func main() {{ "Example" | code }}
```
