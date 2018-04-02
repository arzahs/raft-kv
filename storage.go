package main

import (
	"github.com/dgraph-io/badger"
	"github.com/hashicorp/raft"
)

type BadgerStore struct {
	store *badger.DB
}

type Options struct{
	Path string
	StorageOptions *badger.Options
}

// Create new badger store with default options
func NewStore(path string)(*BadgerStore, error){
	return New(Options{Path: path, StorageOptions: &badger.DefaultOptions})
}

func New(opts Options)(*BadgerStore, error){
	opts.StorageOptions.Dir = opts.Path
	opts.StorageOptions.ValueDir = opts.Path
	db, err := badger.Open(*opts.StorageOptions)
	if err != nil {
		return nil, err
	}

	dbs := BadgerStore{
		store: db,
	}

	return &dbs, err
}


func (dbs *BadgerStore)Set(k, v []byte) error{
	txn := dbs.store.NewTransaction(true)
	defer txn.Discard()

	err := txn.Set(k, v)
	if err != nil {
		return err
	}

	if err := txn.Commit(nil); err != nil {
		return err
	}

	return nil
}

func (dbs *BadgerStore) Get(k []byte) ([]byte, error) {
	var val []byte
	err := dbs.store.View(func(txn *badger.Txn) error {
		item, err := txn.Get(k)
		if err != nil {
			return err
		}
		val, err = item.Value()
		if err != nil {
			return err
		}
		return nil
	})
	return val, err
}

// StoreLog is used to store a single raft log
func (dbs *BadgerStore) StoreLog(log *raft.Log) error {
	return dbs.StoreLogs([]*raft.Log{log})
}

// StoreLogs is used to store a set of raft logs
func (dbs *BadgerStore) StoreLogs(logs []*raft.Log) error {
	txn := dbs.store.NewTransaction(true)

	defer txn.Discard()

	for _, log := range logs {
		key := uint64ToBytes(log.Index)
		val, err := encodeMsgPack(log)
		if err != nil {
			return err
		}
		err = txn.Set(key, val.Bytes())
		if err != nil {
			return err
		}
	}

	if err := txn.Commit(nil); err != nil {
		return err
	}
	return nil
}




