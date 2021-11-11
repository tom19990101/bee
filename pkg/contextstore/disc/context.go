package disc

import (
	"context"

	"github.com/ethersphere/bee/pkg/pusher"
	"github.com/ethersphere/bee/pkg/retrieval"
	"github.com/ethersphere/bee/pkg/storage"
	"github.com/ethersphere/bee/pkg/swarm"
)

var (
	_ storage.SimpleChunkGetter = (*Context)(nil)
	_ storage.SimpleChunkPutter = (*Context)(nil)
)

type Context struct {
	p   *pusher.Service
	chC chan *pusher.Op
	r   retrieval.Interface
}

func New(push *pusher.Service, ret retrieval.Interface) *Context {
	c := &Context{
		p:   push,
		chC: make(chan *pusher.Op),
		r:   ret,
	}

	push.AddFeed(c.chC)
	return c
}

// Put a chunk into the store alongside with its postage stamp. No duplicates
// are allowed. It returns `exists=true` In case the chunk already exists.
func (c *Context) Put(ch swarm.Chunk) (exists bool, err error) {
	op := &pusher.Op{Chunk: ch, Err: make(chan error), Direct: true}

	// TODO needs context
	c.chC <- op

	return false, <-op.Err
}

// Get a chunk by its swarm.Address. Returns the chunk associated with
// the address alongside with its postage stamp, or a storage.ErrNotFound
// if the chunk is not found.
func (c *Context) Get(addr swarm.Address) (swarm.Chunk, error) {
	return c.r.RetrieveChunk(context.TODO(), addr, true)
}
