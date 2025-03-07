// Copyright 2023 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package chunkstore_test

import (
	"context"
	"testing"

	postagetesting "github.com/ethersphere/bee/pkg/postage/testing"
	"github.com/ethersphere/bee/pkg/sharky"
	"github.com/ethersphere/bee/pkg/storage"
	"github.com/ethersphere/bee/pkg/storage/leveldbstore"
	"github.com/ethersphere/bee/pkg/storage/storagetest"
	chunktest "github.com/ethersphere/bee/pkg/storage/testing"
	"github.com/ethersphere/bee/pkg/storer/internal/chunkstore"
	"github.com/ethersphere/bee/pkg/swarm"
	"github.com/spf13/afero"
)

func TestTxChunkStore(t *testing.T) {
	t.Parallel()

	store, err := leveldbstore.New(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}

	sharky, err := sharky.New(&memFS{Fs: afero.NewMemMapFs()}, 1, swarm.SocMaxChunkSize)
	if err != nil {
		t.Fatal(err)
	}

	storagetest.TestTxChunkStore(t, chunkstore.NewTxChunkStore(leveldbstore.NewTxStore(store), sharky))
}

// TestMultipleStampsRefCnt tests the behaviour of ref counting along with multiple
// stamps to ensure transactions work correctly.
func TestMultipleStampsRefCnt(t *testing.T) {
	t.Parallel()

	store, err := leveldbstore.New(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}

	sharky, err := sharky.New(&memFS{Fs: afero.NewMemMapFs()}, 1, swarm.SocMaxChunkSize)
	if err != nil {
		t.Fatal(err)
	}

	chunkStore := chunkstore.NewTxChunkStore(leveldbstore.NewTxStore(store), sharky)
	t.Cleanup(func() {
		if err := chunkStore.Close(); err != nil {
			t.Fatalf("close chunk store: %v", err)
		}
	})

	var (
		chunk  = chunktest.GenerateTestRandomChunk()
		stamps = []swarm.Stamp{
			chunk.Stamp(),
			postagetesting.MustNewStamp(),
			postagetesting.MustNewStamp(),
		}
	)

	verifyAllIndexes := func(t *testing.T) {
		t.Helper()

		rIdx := chunkstore.RetrievalIndexItem{
			Address: chunk.Address(),
		}

		has, err := store.Has(&rIdx)
		if err != nil {
			t.Fatal(err)
		}
		if !has {
			t.Fatalf("retrievalIndex not found %s", chunk.Address())
		}
	}

	t.Run("put with multiple stamps", func(t *testing.T) {
		cs := chunkStore.NewTx(storage.NewTxState(context.TODO()))

		for _, stamp := range stamps {
			err := cs.Put(context.TODO(), chunk.WithStamp(stamp))
			if err != nil {
				t.Fatalf("failed to put chunk: %v", err)
			}
		}

		err := cs.Commit()
		if err != nil {
			t.Fatal(err)
		}

		verifyAllIndexes(t)
	})

	t.Run("rollback delete operations", func(t *testing.T) {
		t.Run("less than refCnt", func(t *testing.T) {
			cs := chunkStore.NewTx(storage.NewTxState(context.TODO()))

			for i := 0; i < len(stamps)-1; i++ {
				err := cs.Delete(context.TODO(), chunk.Address())
				if err != nil {
					t.Fatalf("failed to delete chunk %d: %v", i, err)
				}
			}

			err := cs.Rollback()
			if err != nil {
				t.Fatal(err)
			}

			verifyAllIndexes(t)
		})

		// this should remove all the stamps and hopefully bring them back
		t.Run("till refCnt", func(t *testing.T) {
			cs := chunkStore.NewTx(storage.NewTxState(context.TODO()))

			for i := 0; i < len(stamps); i++ {
				err := cs.Delete(context.TODO(), chunk.Address())
				if err != nil {
					t.Fatalf("failed to delete chunk %d: %v", i, err)
				}
			}

			err := cs.Rollback()
			if err != nil {
				t.Fatal(err)
			}

			verifyAllIndexes(t)
		})
	})
}
