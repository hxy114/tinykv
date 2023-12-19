package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	db *badger.DB
	// Your Data Here (1).
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{db: engine_util.CreateDB(conf.DBPath, false)}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if err := s.db.Close(); err != nil {
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.db.NewTransaction(false)
	sr := NewStandAloneStorageReader(txn)
	return sr, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).

	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			put := m.Data.(storage.Put)
			err := engine_util.PutCF(s.db, put.Cf, put.Key, put.Value)
			if err != nil {
				return err
			}
		case storage.Delete:
			delete := m.Data.(storage.Delete)
			err := engine_util.DeleteCF(s.db, delete.Cf, delete.Key)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func NewStandAloneStorageReader(txn *badger.Txn) *StandAloneStorageReader {
	return &StandAloneStorageReader{
		txn: txn,
	}
}

func (sr *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(sr.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (sr *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return NewStandAloneStorageIterator(engine_util.NewCFIterator(cf, sr.txn))
}

func (sr *StandAloneStorageReader) Close() {
	sr.txn.Discard()
}

type StandAloneStorageIterator struct {
	iter *engine_util.BadgerIterator
}

func NewStandAloneStorageIterator(iter *engine_util.BadgerIterator) *StandAloneStorageIterator {
	return &StandAloneStorageIterator{
		iter: iter,
	}
}

func (si *StandAloneStorageIterator) Item() engine_util.DBItem {
	return si.iter.Item()
}

func (si *StandAloneStorageIterator) Valid() bool {
	if !si.iter.Valid() {
		return false
	}
	return true
}

func (si *StandAloneStorageIterator) Close() {
	si.iter.Close()
}

func (si *StandAloneStorageIterator) Next() {
	si.iter.Next()
}

func (si *StandAloneStorageIterator) Seek(key []byte) {
	si.iter.Seek(key)
}

func (si *StandAloneStorageIterator) Rewind() {
	si.iter.Rewind()
}
