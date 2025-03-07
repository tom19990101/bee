// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package postage_test

import (
	"context"
	crand "crypto/rand"
	"errors"
	"io"
	"math/big"
	"testing"
	"time"

	"github.com/ethersphere/bee/pkg/postage"
	pstoremock "github.com/ethersphere/bee/pkg/postage/batchstore/mock"
	postagetesting "github.com/ethersphere/bee/pkg/postage/testing"
	"github.com/ethersphere/bee/pkg/storage/inmemstore"
	"github.com/google/go-cmp/cmp"
)

// TestSaveLoad tests the idempotence of saving and loading the postage.Service
// with all the active stamp issuers.
func TestSaveLoad(t *testing.T) {
	store := inmemstore.New()
	defer store.Close()
	pstore := pstoremock.New()
	saved := func(id int64) postage.Service {
		ps := postage.NewService(store, pstore, id)
		for i := 0; i < 16; i++ {
			err := ps.Add(newTestStampIssuer(t, 1000))
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := ps.Close(); err != nil {
			t.Fatal(err)
		}
		return ps
	}
	loaded := func(id int64) postage.Service {
		return postage.NewService(store, pstore, id)
	}
	test := func(id int64) {
		psS := saved(id)
		psL := loaded(id)

		sMap := map[string]struct{}{}
		stampIssuers, err := psS.StampIssuers()
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range stampIssuers {
			sMap[string(s.ID())] = struct{}{}
		}

		stampIssuers, err = psL.StampIssuers()
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range stampIssuers {
			if _, ok := sMap[string(s.ID())]; !ok {
				t.Fatalf("mismatch between saved and loaded")
			}
		}
	}
	test(0)
	test(1)
}

func TestGetStampIssuer(t *testing.T) {
	store := inmemstore.New()
	defer store.Close()
	chainID := int64(0)
	testChainState := postagetesting.NewChainState()
	if testChainState.Block < uint64(postage.BlockThreshold) {
		testChainState.Block += uint64(postage.BlockThreshold + 1)
	}
	validBlockNumber := testChainState.Block - uint64(postage.BlockThreshold+1)
	pstore := pstoremock.New(pstoremock.WithChainState(testChainState))
	ps := postage.NewService(store, pstore, chainID)
	ids := make([][]byte, 8)
	for i := range ids {
		id := make([]byte, 32)
		_, err := io.ReadFull(crand.Reader, id)
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = id
		if i == 0 {
			continue
		}

		var shift uint64 = 0
		if i > 3 {
			shift = uint64(i)
		}
		err = ps.Add(postage.NewStampIssuer(
			string(id),
			"",
			id,
			big.NewInt(3),
			16,
			8,
			validBlockNumber+shift, true),
		)
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Run("found", func(t *testing.T) {
		for _, id := range ids[1:4] {
			st, save, err := ps.GetStampIssuer(context.Background(), id)
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			_ = save(true)
			if st.Label() != string(id) {
				t.Fatalf("wrong issuer returned")
			}
		}

		// check if the save() call persisted the stamp issuers
		for _, id := range ids[1:4] {
			stampIssuerItem := postage.NewStampIssuerItem(id)
			err := store.Get(stampIssuerItem)
			if err != nil {
				t.Fatal(err)
			}
			if string(id) != stampIssuerItem.ID() {
				t.Fatalf("got id %s, want id %s", stampIssuerItem.ID(), string(id))
			}
		}
	})
	t.Run("not found", func(t *testing.T) {
		_, _, err := ps.GetStampIssuer(context.Background(), ids[0])
		if !errors.Is(err, postage.ErrNotFound) {
			t.Fatalf("expected ErrNotFound, got %v", err)
		}
	})
	t.Run("not usable", func(t *testing.T) {
		for _, id := range ids[4:] {
			_, _, err := ps.GetStampIssuer(context.Background(), id)
			if !errors.Is(err, postage.ErrNotUsable) {
				t.Fatalf("expected ErrNotUsable, got %v", err)
			}
		}
	})
	t.Run("recovered", func(t *testing.T) {
		b := postagetesting.MustNewBatch()
		b.Start = validBlockNumber
		testAmount := big.NewInt(1)
		err := ps.HandleCreate(b, testAmount)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		st, sv, err := ps.GetStampIssuer(context.Background(), b.ID)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if st.Label() != "recovered" {
			t.Fatal("wrong issuer returned")
		}
		err = sv(true)
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("topup", func(t *testing.T) {
		err := ps.HandleTopUp(ids[1], big.NewInt(10))
		if err != nil {
			t.Fatal(err)
		}
		stampIssuer, save, err := ps.GetStampIssuer(context.Background(), ids[1])
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		_ = save(true)
		if stampIssuer.Amount().Cmp(big.NewInt(13)) != 0 {
			t.Fatalf("expected amount %d got %d", 13, stampIssuer.Amount().Int64())
		}
	})
	t.Run("dilute", func(t *testing.T) {
		err := ps.HandleDepthIncrease(ids[2], 17)
		if err != nil {
			t.Fatal(err)
		}
		stampIssuer, save, err := ps.GetStampIssuer(context.Background(), ids[2])
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		_ = save(true)
		if stampIssuer.Amount().Cmp(big.NewInt(3)) != 0 {
			t.Fatalf("expected amount %d got %d", 3, stampIssuer.Amount().Int64())
		}
		if stampIssuer.Depth() != 17 {
			t.Fatalf("expected depth %d got %d", 17, stampIssuer.Depth())
		}
	})
	t.Run("in use", func(t *testing.T) {
		_, save1, err := ps.GetStampIssuer(context.Background(), ids[1])
		if err != nil {
			t.Fatal(err)
		}
		_, save2, err := ps.GetStampIssuer(context.Background(), ids[2])
		if err != nil {
			t.Fatal(err)
		}
		_ = save2(true)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		_, _, err = ps.GetStampIssuer(ctx, ids[1])
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected context.DeadlineExceeded, got %v", err)
		}
		// ensure we get access once the first one is saved
		done := make(chan struct{})
		errC := make(chan error, 1)
		go func() {
			_, save12, err := ps.GetStampIssuer(context.Background(), ids[1])
			if err != nil {
				errC <- err
				return
			}
			_ = save12(true)
			close(done)
		}()
		_ = save1(true)
		select {
		case <-done:
		case err := <-errC:
			t.Fatal(err)
		case <-time.After(time.Second):
			t.Fatal("timeout")
		}
	})
	t.Run("save without update", func(t *testing.T) {
		is, save, err := ps.GetStampIssuer(context.Background(), ids[1])
		if err != nil {
			t.Fatal(err)
		}
		data := is.Buckets()
		modified := make([]uint32, len(data))
		copy(modified, data)
		for k, b := range modified {
			b++
			modified[k] = b
		}

		err = save(false)
		if err != nil {
			t.Fatal(err)
		}

		is, _, err = ps.GetStampIssuer(context.Background(), ids[1])
		if err != nil {
			t.Fatal(err)
		}

		if !cmp.Equal(is.Buckets(), data) {
			t.Fatal("expected buckets to be unchanged")
		}

	})
}
