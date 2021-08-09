package standalone_storage

import (
	"errors"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"os"
	"path/filepath"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
}

type StandAloneReader struct {
	txn *badger.Txn
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	path := filepath.Join(conf.DBPath, "kv")
	if err := os.MkdirAll(path, os.ModePerm); err != nil {
		panic(err)
	}
	return &StandAloneStorage{engine_util.CreateDB(path, false)}
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(_ *kvrpcpb.Context) (storage.StorageReader, error) {
	txn := s.db.NewTransaction(false)
	return &StandAloneReader{
		txn: txn,
	}, nil
}

func (s *StandAloneStorage) Write(_ *kvrpcpb.Context, batch []storage.Modify) error {
	return s.db.Update(func(txn *badger.Txn) error {
		for _, modify := range batch {
			var err error
			switch t := modify.Data.(type) {
			case storage.Delete:
				err = txn.Delete(engine_util.KeyWithCF(t.Cf, t.Key))
			case storage.Put:
				err = txn.Set(engine_util.KeyWithCF(t.Cf, t.Key), t.Value)
			default:
				err = errors.New("unsupported write operation")
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *StandAloneReader) GetCF(cf string, key []byte) (dist []byte, err error) {
	item, err := s.txn.Get(engine_util.KeyWithCF(cf, key))
	if err != nil {
		if err == badger.ErrKeyNotFound {
			err = nil
		}
		return
	}
	if item.IsEmpty() {
		return
	}
	return item.ValueCopy(dist)
}

func (s *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)
}

func (s *StandAloneReader) Close() {
	s.txn.Discard()
}
