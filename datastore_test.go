package boltds

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	ds "github.com/ipfs/go-datastore"
	dstest "github.com/ipfs/go-datastore/test"
)

func TestSuite(t *testing.T) {
	path, err := ioutil.TempDir("/tmp", "boltdbtest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(path)
	db, err := NewBoltDatastore(path, "test", false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	dstest.SubtestAll(t, db)
}

func TestBasicPutGet(t *testing.T) {
	path, err := ioutil.TempDir("/tmp", "boltdbtest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(path)
	db, err := NewBoltDatastore(path, "test", false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	dsk := ds.NewKey("test")
	somedata := []byte("some data in the datastore")

	err = db.Put(dsk, somedata)
	if err != nil {
		t.Fatal(err)
	}

	b, err := db.Get(dsk)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(b, somedata) {
		t.Fatal("wrong data")
	}

	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func BenchmarkPut(b *testing.B) {
	b.StopTimer()
	path, err := ioutil.TempDir("/tmp", "boltdbtest")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(path)
	db, err := NewBoltDatastore(path, "test", false)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	values := make(map[string][]byte)
	for i := 0; i < b.N; i++ {
		values[fmt.Sprint(i)] = []byte(fmt.Sprintf("value number %d", i))
	}

	b.StartTimer()

	for k, v := range values {

		dsk := ds.NewKey(k)
		err := db.Put(dsk, v)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}
