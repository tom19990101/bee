package upload

import (
	"github.com/ethersphere/bee/pkg/contextstore/nested"
	"github.com/ethersphere/bee/pkg/pusher"
	"github.com/ethersphere/bee/pkg/storage"
	"github.com/ethersphere/bee/pkg/swarm"
)

type Context struct {
	stores *nested.Store
	quit   chan struct{}
}

func New(base string) *Context {
	u := &Context{
		stores: nested.Open(base),
	}

	return u
}

func (c *Context) FeedPusher() chan *pusher.Op {
	go func() {
	}()
	return nil
}

func (c *Context) Get(addr swarm.Address) (swarm.Chunk, error) {
	return c.stores.Get(addr)
}

func (c *Context) GetByName(name string) (storage.SimpleChunkStorer, error) {
	return c.stores.GetByName(name)
}

func (c *Context) New() (string, storage.SimpleChunkStorer, error) {
	return c.stores.New()
}
