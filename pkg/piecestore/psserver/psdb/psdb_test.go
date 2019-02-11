// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package psdb

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"

	"storj.io/storj/internal/testcontext"
	"storj.io/storj/internal/testidentity"
	"storj.io/storj/pkg/bwagreement/testbwagreement"
	"storj.io/storj/pkg/identity"
	"storj.io/storj/pkg/pb"
)

const concurrency = 10

func newDB(t testing.TB, id string) (*DB, func()) {
	tmpdir, err := ioutil.TempDir("", "storj-psdb-"+id)
	if err != nil {
		t.Fatal(err)
	}
	dbpath := filepath.Join(tmpdir, "psdb.db")

	db, err := Open(dbpath)
	if err != nil {
		t.Fatal(err)
	}

	return db, func() {
		err := db.Close()
		if err != nil {
			t.Fatal(err)
		}
		err = os.RemoveAll(tmpdir)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func newTestID(ctx context.Context, t *testing.T) *identity.FullIdentity {
	id, err := testidentity.NewTestIdentity(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestNewInmemory(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestHappyPath(t *testing.T) {
	db, cleanup := newDB(t, "1")
	defer cleanup()

	type TTL struct {
		ID         string
		Expiration int64
	}

	tests := []TTL{
		{ID: "", Expiration: 0},
		{ID: "\x00", Expiration: ^int64(0)},
		{ID: "test", Expiration: 666},
	}

	t.Run("Create", func(t *testing.T) {
		for P := 0; P < concurrency; P++ {
			t.Run("#"+strconv.Itoa(P), func(t *testing.T) {
				t.Parallel()
				for _, ttl := range tests {
					err := db.AddTTL(ttl.ID, ttl.Expiration, 0)
					if err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	})

	t.Run("Get", func(t *testing.T) {
		for P := 0; P < concurrency; P++ {
			t.Run("#"+strconv.Itoa(P), func(t *testing.T) {
				t.Parallel()
				for _, ttl := range tests {
					expiration, err := db.GetTTLByID(ttl.ID)
					if err != nil {
						t.Fatal(err)
					}

					if ttl.Expiration != expiration {
						t.Fatalf("expected %d got %d", ttl.Expiration, expiration)
					}
				}
			})
		}
	})

	t.Run("Delete", func(t *testing.T) {
		for P := 0; P < concurrency; P++ {
			t.Run("Delete", func(t *testing.T) {
				t.Parallel()
				for _, ttl := range tests {
					err := db.DeleteTTLByID(ttl.ID)
					if err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	})

	t.Run("Get Deleted", func(t *testing.T) {
		for P := 0; P < concurrency; P++ {
			t.Run("#"+strconv.Itoa(P), func(t *testing.T) {
				t.Parallel()
				for _, ttl := range tests {
					expiration, err := db.GetTTLByID(ttl.ID)
					if err == nil {
						t.Fatal(err)
					}
					if expiration != 0 {
						t.Fatalf("expected expiration 0 got %d", expiration)
					}
				}
			})
		}
	})

	bandwidthAllocation := func(ctx context.Context, action pb.BandwidthAction, total int64, expiration time.Duration) *pb.RenterBandwidthAllocation {
		snID, upID := newTestID(ctx, t), newTestID(ctx, t)
		pba, err := testbwagreement.GeneratePayerBandwidthAllocation(action, snID, upID, time.Hour)
		require.NoError(t, err)

		rba, err := testbwagreement.GenerateRenterBandwidthAllocation(pba, snID.ID, upID, total)
		require.NoError(t, err)
		return rba
	}

	ctx := testcontext.New(t)
	defer ctx.Cleanup()

	allocationTests := []*pb.RenterBandwidthAllocation{
		bandwidthAllocation(ctx, pb.BandwidthAction_PUT, 10, 30*time.Second),
		bandwidthAllocation(ctx, pb.BandwidthAction_GET, 100, 300*time.Second),
		bandwidthAllocation(ctx, pb.BandwidthAction_PUT, 1000, 3*time.Second),
		bandwidthAllocation(ctx, pb.BandwidthAction_GET, 1, 30*time.Second),
	}

	type BWUSAGE struct {
		size    int64
		timenow time.Time
	}

	bwtests := []BWUSAGE{
		// size is total size stored
		{size: 11110, timenow: time.Now()},
	}

	t.Run("Bandwidth Allocation", func(t *testing.T) {

		for P := 0; P < concurrency; P++ {
			t.Run("#"+strconv.Itoa(P), func(t *testing.T) {
				t.Parallel()
				for _, test := range allocationTests {
					err := db.WriteBandwidthAllocToDB(test)
					if err != nil {
						t.Fatal(err)
					}

					agreements, err := db.GetBandwidthAllocationBySignature(test.Signature)
					if err != nil {
						t.Fatal(err)
					}

					found := false
					for _, agreement := range agreements {
						if pb.Equal(agreement, test) {
							found = true
							break
						}
					}

					if !found {
						t.Fatal("did not find added bandwidth allocation")
					}
				}
			})
		}
	})

	t.Run("Get all Bandwidth Allocations", func(t *testing.T) {
		for P := 0; P < concurrency; P++ {
			t.Run("#"+strconv.Itoa(P), func(t *testing.T) {
				t.Parallel()

				agreementGroups, err := db.GetBandwidthAllocations()
				if err != nil {
					t.Fatal(err)
				}

				found := false
				for _, agreements := range agreementGroups {
					for _, agreement := range agreements {
						for _, test := range allocationTests {
							if pb.Equal(&agreement.Agreement, test) {
								found = true
								break
							}
						}
					}
				}

				if !found {
					t.Fatal("did not find added bandwidth allocation")
				}
			})
		}
	})

	t.Run("GetBandwidthUsedByDay", func(t *testing.T) {
		for P := 0; P < concurrency; P++ {
			t.Run("#"+strconv.Itoa(P), func(t *testing.T) {
				t.Parallel()
				for _, bw := range bwtests {
					size, err := db.GetBandwidthUsedByDay(bw.timenow)
					if err != nil {
						t.Fatal(err)
					}
					if bw.size != size {
						t.Fatalf("expected %d got %d", bw.size, size)
					}
				}
			})
		}
	})

	t.Run("GetTotalBandwidthBetween", func(t *testing.T) {
		for P := 0; P < concurrency; P++ {
			t.Run("#"+strconv.Itoa(P), func(t *testing.T) {
				t.Parallel()
				for _, bw := range bwtests {
					size, err := db.GetTotalBandwidthBetween(bw.timenow, bw.timenow)
					if err != nil {
						t.Fatal(err)
					}
					if bw.size != size {
						t.Fatalf("expected %d got %d", bw.size, size)
					}
				}
			})
		}
	})
}

func BenchmarkWriteBandwidthAllocation(b *testing.B) {
	db, cleanup := newDB(b, "3")
	defer cleanup()
	const WritesPerLoop = 10
	b.RunParallel(func(b *testing.PB) {
		for b.Next() {
			for i := 0; i < WritesPerLoop; i++ {
				_ = db.WriteBandwidthAllocToDB(&pb.RenterBandwidthAllocation{
					PayerAllocation: pb.PayerBandwidthAllocation{},
					Total:           156,
					Signature:       []byte("signed by test"),
				})
			}
		}
	})
}
