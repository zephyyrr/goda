package goda

import (
	"io"
)

type Storer interface {
	Store(interface{}) error
	io.Closer
}

type Retriever interface {
	Retrieve(interface{}) error
	io.Closer
}