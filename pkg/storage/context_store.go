package storage

import (
	"errors"

	"github.com/ethersphere/bee/pkg/swarm"
)

var ErrNoContext = errors.New("no such context")

type ContextStorer interface {
	ContextByName(Context, string) (string, SimpleChunkStorer, error)
	LookupContext() SimpleChunkGetter
}

type Context int

const (
	ContextSync Context = iota + 1
	ContextCache
	ContextPin
	ContextUpload
	ContextDisc
)

type SimpleChunkStorer interface {
	SimpleChunkPutter
	SimpleChunkGetter
	// Iterate over chunks in no particular order.
	Iterate(IterateChunkFn) error
	// Delete a chunk from the store.
	Delete(swarm.Address) error
	// Has checks whether a chunk exists in the store.
	Has(swarm.Address) (bool, error)
}

type SimpleChunkGetter interface {
	// Get a chunk by its swarm.Address. Returns the chunk associated with
	// the address alongside with its postage stamp, or a storage.ErrNotFound
	// if the chunk is not found.
	Get(addr swarm.Address) (swarm.Chunk, error)
}

type SimpleChunkPutter interface {
	// Put a chunk into the store alongside with its postage stamp. No duplicates
	// are allowed. It returns `exists=true` In case the chunk already exists.
	Put(ch swarm.Chunk) (exists bool, err error)
}

type IterateChunkFn func(swarm.Chunk) (stop bool, err error)
