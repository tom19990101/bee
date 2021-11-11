package nested

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/ethersphere/bee/pkg/contextstore/simplestore"
	"github.com/ethersphere/bee/pkg/storage"
	"github.com/ethersphere/bee/pkg/swarm"
	"github.com/google/uuid"
)

type Store struct {
	stores map[string]*simplestore.Store
	mtx    sync.Mutex

	base string // the base working directory
}

func Open(base string) *Store {
	s := &Store{
		stores: make(map[string]*simplestore.Store),
		base:   base,
	}

	// populate all stores in the basedir
	entries, err := os.ReadDir(base)
	if err != nil {
		panic(err)
	}

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		// try to open the store
		st, err := simplestore.New(filepath.Join(base, e.Name()))
		if err != nil {
			panic(err)
		}
		s.stores[e.Name()] = st
	}

	return s
}

func (s *Store) GetByName(name string) (storage.SimpleChunkStorer, error) {
	// todo protection with a waitgroup
	// so that store deletion does not occur while other goroutines are Putting into the store
	// or when other consumers read, eg from the API
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if st, found := s.stores[name]; found {
		return st, nil
	}
	return nil, storage.ErrNoContext
}

func (s *Store) List() (stores []string) {
	for k := range s.stores {
		stores = append(stores, k)
	}
	return stores
}

func (s *Store) New() (string, storage.SimpleChunkStorer, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	guid := uuid.NewString()
	st, err := simplestore.New(filepath.Join(s.base, guid))
	if err != nil {
		panic(err)
	}
	s.stores[guid] = st
	return guid, st, nil
}

// Get a chunk by its swarm.Address. Returns the chunk associated with
// the address alongside with its postage stamp, or a storage.ErrNotFound
// if the chunk is not found. It checks all associated stores for the chunk.
func (s *Store) Get(addr swarm.Address) (swarm.Chunk, error) {
	// try in all stores
	s.mtx.Lock()
	defer s.mtx.Unlock()

	for _, s := range s.stores {
		if ch, err := s.Get(addr); err == nil {
			return ch, nil
		}
	}
	return nil, storage.ErrNotFound
}

// Delete a nested store with the given name.
// This is irreversible and the data will be forever gone.
func (s *Store) Delete(name string) error {
	// lock, close the file, delete the subdir
	panic("not implemented")
}
