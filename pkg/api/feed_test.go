// Copyright 2021 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package api_test

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"testing"

	"github.com/ethersphere/bee/pkg/api"
	"github.com/ethersphere/bee/pkg/feeds"
	"github.com/ethersphere/bee/pkg/file/loadsave"
	"github.com/ethersphere/bee/pkg/jsonhttp"
	"github.com/ethersphere/bee/pkg/jsonhttp/jsonhttptest"
	"github.com/ethersphere/bee/pkg/log"
	"github.com/ethersphere/bee/pkg/manifest"
	"github.com/ethersphere/bee/pkg/postage"
	mockpost "github.com/ethersphere/bee/pkg/postage/mock"
	testingsoc "github.com/ethersphere/bee/pkg/soc/testing"
	mockstorer "github.com/ethersphere/bee/pkg/storer/mock"
	"github.com/ethersphere/bee/pkg/swarm"
)

const ownerString = "8d3766440f0d7b949a5e32995d09619a7f86e632"

var expReference = swarm.MustParseHexAddress("891a1d1c8436c792d02fc2e8883fef7ab387eaeaacd25aa9f518be7be7856d54")

func TestFeed_Get(t *testing.T) {
	t.Parallel()

	var (
		feedResource = func(owner, topic, at string) string {
			if at != "" {
				return fmt.Sprintf("/feeds/%s/%s?at=%s", owner, topic, at)
			}
			return fmt.Sprintf("/feeds/%s/%s", owner, topic)
		}
		mockStorer = mockstorer.New()
	)

	t.Run("with at", func(t *testing.T) {
		t.Parallel()

		var (
			timestamp       = int64(12121212)
			ch              = toChunk(t, uint64(timestamp), expReference.Bytes())
			look            = newMockLookup(12, 0, ch, nil, &id{}, &id{})
			factory         = newMockFactory(look)
			idBytes, _      = (&id{}).MarshalBinary()
			client, _, _, _ = newTestServer(t, testServerOptions{
				Storer: mockStorer,
				Feeds:  factory,
			})
		)

		jsonhttptest.Request(t, client, http.MethodGet, feedResource(ownerString, "aabbcc", "12"), http.StatusOK,
			jsonhttptest.WithExpectedJSONResponse(api.FeedReferenceResponse{Reference: expReference}),
			jsonhttptest.WithExpectedResponseHeader(api.SwarmFeedIndexHeader, hex.EncodeToString(idBytes)),
		)
	})

	t.Run("latest", func(t *testing.T) {
		t.Parallel()

		var (
			timestamp  = int64(12121212)
			ch         = toChunk(t, uint64(timestamp), expReference.Bytes())
			look       = newMockLookup(-1, 2, ch, nil, &id{}, &id{})
			factory    = newMockFactory(look)
			idBytes, _ = (&id{}).MarshalBinary()

			client, _, _, _ = newTestServer(t, testServerOptions{
				Storer: mockStorer,
				Feeds:  factory,
			})
		)

		jsonhttptest.Request(t, client, http.MethodGet, feedResource(ownerString, "aabbcc", ""), http.StatusOK,
			jsonhttptest.WithExpectedJSONResponse(api.FeedReferenceResponse{Reference: expReference}),
			jsonhttptest.WithExpectedResponseHeader(api.SwarmFeedIndexHeader, hex.EncodeToString(idBytes)),
		)
	})
}

// nolint:paralleltest
func TestFeed_Post(t *testing.T) {
	// post to owner, tpoic, then expect a reference
	// get the reference from the store, unmarshal to a
	// manifest entry and make sure all metadata correct
	var (
		logger          = log.Noop
		topic           = "aabbcc"
		mp              = mockpost.New(mockpost.WithIssuer(postage.NewStampIssuer("", "", batchOk, big.NewInt(3), 11, 10, 1000, true)))
		mockStorer      = mockstorer.New()
		client, _, _, _ = newTestServer(t, testServerOptions{
			Storer: mockStorer,
			Logger: logger,
			Post:   mp,
		})
		url = fmt.Sprintf("/feeds/%s/%s?type=%s", ownerString, topic, "sequence")
	)
	t.Run("ok", func(t *testing.T) {
		jsonhttptest.Request(t, client, http.MethodPost, url, http.StatusCreated,
			jsonhttptest.WithRequestHeader(api.SwarmDeferredUploadHeader, "true"),
			jsonhttptest.WithRequestHeader(api.SwarmPostageBatchIdHeader, batchOkStr),
			jsonhttptest.WithExpectedJSONResponse(api.FeedReferenceResponse{
				Reference: expReference,
			}),
		)

		ls := loadsave.NewReadonly(mockStorer.ChunkStore())
		i, err := manifest.NewMantarayManifestReference(expReference, ls)
		if err != nil {
			t.Fatal(err)
		}
		e, err := i.Lookup(context.Background(), "/")
		if err != nil {
			t.Fatal(err)
		}

		meta := e.Metadata()
		if e := meta[api.FeedMetadataEntryOwner]; e != ownerString {
			t.Fatalf("owner mismatch. got %s want %s", e, ownerString)
		}
		if e := meta[api.FeedMetadataEntryTopic]; e != topic {
			t.Fatalf("topic mismatch. got %s want %s", e, topic)
		}
		if e := meta[api.FeedMetadataEntryType]; e != "Sequence" {
			t.Fatalf("type mismatch. got %s want %s", e, "Sequence")
		}
	})
	t.Run("postage", func(t *testing.T) {
		t.Run("err - bad batch", func(t *testing.T) {
			hexbatch := hex.EncodeToString(batchInvalid)
			jsonhttptest.Request(t, client, http.MethodPost, url, http.StatusNotFound,
				jsonhttptest.WithRequestHeader(api.SwarmPostageBatchIdHeader, hexbatch),
				jsonhttptest.WithExpectedJSONResponse(jsonhttp.StatusResponse{
					Message: "batch with id not found",
					Code:    http.StatusNotFound,
				}))
		})

		t.Run("ok - batch zeros", func(t *testing.T) {
			hexbatch := hex.EncodeToString(batchOk)
			jsonhttptest.Request(t, client, http.MethodPost, url, http.StatusCreated,
				jsonhttptest.WithRequestHeader(api.SwarmDeferredUploadHeader, "true"),
				jsonhttptest.WithRequestHeader(api.SwarmPostageBatchIdHeader, hexbatch),
			)
		})
		t.Run("bad request - batch empty", func(t *testing.T) {
			hexbatch := hex.EncodeToString(batchEmpty)
			jsonhttptest.Request(t, client, http.MethodPost, url, http.StatusBadRequest,
				jsonhttptest.WithRequestHeader(api.SwarmPostageBatchIdHeader, hexbatch),
			)
		})
	})

}

// TestDirectUploadFeed tests that the direct upload endpoint give correct error message in dev mode
func TestFeedDirectUpload(t *testing.T) {
	t.Parallel()
	var (
		topic           = "aabbcc"
		mp              = mockpost.New(mockpost.WithIssuer(postage.NewStampIssuer("", "", batchOk, big.NewInt(3), 11, 10, 1000, true)))
		mockStorer      = mockstorer.New()
		client, _, _, _ = newTestServer(t, testServerOptions{
			Storer:  mockStorer,
			Post:    mp,
			BeeMode: api.DevMode,
		})
		url = fmt.Sprintf("/feeds/%s/%s?type=%s", ownerString, topic, "sequence")
	)
	jsonhttptest.Request(t, client, http.MethodPost, url, http.StatusBadRequest,
		jsonhttptest.WithRequestHeader(api.SwarmDeferredUploadHeader, "false"),
		jsonhttptest.WithRequestHeader(api.SwarmPostageBatchIdHeader, batchOkStr),
		jsonhttptest.WithExpectedJSONResponse(jsonhttp.StatusResponse{
			Message: api.ErrUnsupportedDevNodeOperation.Error(),
			Code:    http.StatusBadRequest,
		}),
	)
}

type factoryMock struct {
	sequenceCalled bool
	epochCalled    bool
	feed           *feeds.Feed
	lookup         feeds.Lookup
}

func newMockFactory(mockLookup feeds.Lookup) *factoryMock {
	return &factoryMock{lookup: mockLookup}
}

func (f *factoryMock) NewLookup(t feeds.Type, feed *feeds.Feed) (feeds.Lookup, error) {
	switch t {
	case feeds.Sequence:
		f.sequenceCalled = true
	case feeds.Epoch:
		f.epochCalled = true
	}
	f.feed = feed
	return f.lookup, nil
}

type mockLookup struct {
	at, after int64
	chunk     swarm.Chunk
	err       error
	cur, next feeds.Index
}

func newMockLookup(at, after int64, ch swarm.Chunk, err error, cur, next feeds.Index) *mockLookup {
	return &mockLookup{at: at, after: after, chunk: ch, err: err, cur: cur, next: next}
}

func (l *mockLookup) At(_ context.Context, at, after int64) (swarm.Chunk, feeds.Index, feeds.Index, error) {
	if l.at == -1 {
		// shortcut to ignore the value in the call since time.Now() is a moving target
		return l.chunk, l.cur, l.next, nil
	}
	if at == l.at && after == l.after {
		return l.chunk, l.cur, l.next, nil
	}
	return nil, nil, nil, errors.New("no feed update found")
}

func toChunk(t *testing.T, at uint64, payload []byte) swarm.Chunk {
	t.Helper()

	ts := make([]byte, 8)
	binary.BigEndian.PutUint64(ts, at)
	content := append(ts, payload...)

	s := testingsoc.GenerateMockSOC(t, content)
	return s.Chunk()
}

type id struct{}

func (i *id) MarshalBinary() ([]byte, error) {
	return []byte("accd"), nil
}

func (i *id) String() string {
	return "44237"
}

func (*id) Next(last int64, at uint64) feeds.Index {
	return &id{}
}
