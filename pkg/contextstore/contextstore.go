package contextstore

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/ethersphere/bee/pkg/contextstore/disc"
	"github.com/ethersphere/bee/pkg/contextstore/upload"
	"github.com/ethersphere/bee/pkg/pusher"
	"github.com/ethersphere/bee/pkg/retrieval"
	"github.com/ethersphere/bee/pkg/storage"
	"github.com/ethersphere/bee/pkg/swarm"
)

var errUnsupported = errors.New("unsupported operation")

type ContextStore struct {
	uploadContext *upload.Context
	discContext   *disc.Context
	ls            storage.Storer
}

func New(path string, ls storage.Storer, push *pusher.Service, ret retrieval.Interface) *ContextStore {
	if err := os.Mkdir(filepath.Join(path, "upload"), 0755); err != nil {
		panic(err)
	}
	up := upload.New(filepath.Join(path, "upload"))
	c := &ContextStore{
		uploadContext: up,
		ls:            ls,
		discContext:   disc.New(push, ret),
	}
	return c
}

// ContextByName returns a writable context with the supplied name.
// When name is an empty string, a new context will be created.
func (c *ContextStore) ContextByName(sc storage.Context, name string) (string, storage.SimpleChunkStorer, error) {
	switch sc {
	case storage.ContextUpload:
		if name == "" {
			return c.uploadContext.New()
		}
		st, err := c.uploadContext.GetByName(name)
		return "", st, err
	case storage.ContextPin:
		//...
	}
	return "", nil, errUnsupported
}

func (c *ContextStore) ReadContext(sc storage.Context) storage.SimpleChunkGetter {
	switch sc {
	case storage.ContextUpload:
		return c.uploadContext
	case storage.ContextSync:
		wrap(c.ls, storage.ModeGetSync, storage.ModePutSync)

	}
	return nil
}

func (c *ContextStore) PinContext() storage.SimpleChunkStorer {
	return wrap(c.ls, storage.ModeGetRequest, storage.ModePutRequest)
}

func (c *ContextStore) DiscReadContext() storage.SimpleChunkGetter {
	return c.discContext
}

func (c *ContextStore) LookupContext() storage.SimpleChunkGetter {
	return &syntheticStore{
		seq: []storage.SimpleChunkGetter{
			c.ReadContext(storage.ContextUpload),
			c.ReadContext(storage.ContextSync),
			c.ReadContext(storage.ContextDisc),
		},
	}
}

type syntheticStore struct {
	seq []storage.SimpleChunkGetter
}

func (s *syntheticStore) Get(addr swarm.Address) (swarm.Chunk, error) {
	for _, st := range s.seq {
		if ch, err := st.Get(addr); err != nil {
			return ch, err
		}
	}
	return nil, storage.ErrNotFound
}

type modeWrapper struct {
	modeGet storage.ModeGet
	modePut storage.ModePut
	ls      storage.Storer
}

func wrap(l storage.Storer, g storage.ModeGet, p storage.ModePut) storage.SimpleChunkStorer {
	return &modeWrapper{
		modeGet: g,
		modePut: p,
		ls:      l,
	}
}

// Get a chunk by its swarm.Address. Returns the chunk associated with
// the address alongside with its postage stamp, or a storage.ErrNotFound
// if the chunk is not found.
func (m *modeWrapper) Get(addr swarm.Address) (swarm.Chunk, error) {
	return m.ls.Get(context.TODO(), m.modeGet, addr)
}

// Put a chunk into the store alongside with its postage stamp. No duplicates
// are allowed. It returns `exists=true` In case the chunk already exists.
func (m *modeWrapper) Put(ch swarm.Chunk) (exists bool, err error) {
	e, err := m.ls.Put(context.TODO(), m.modePut, ch)
	return e[0], err
}

// Iterate over chunks in no particular order.
func (m *modeWrapper) Iterate(_ storage.IterateChunkFn) error {
	panic("not implemented") // TODO: Implement
}

// Delete a chunk from the store.
func (m *modeWrapper) Delete(addr swarm.Address) error {
	return m.ls.Set(context.TODO(), storage.ModeSetRemove, addr)
}

// Has checks whether a chunk exists in the store.
func (m *modeWrapper) Has(addr swarm.Address) (bool, error) {
	return m.ls.Has(context.TODO(), addr)
}
