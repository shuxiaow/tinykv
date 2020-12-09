package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	Kv     *badger.DB
	KvPath string
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	storage := &StandAloneStorage{
		Kv:     engine_util.CreateDB(conf.DBPath, conf.Raft),
		KvPath: conf.DBPath,
	}
	return storage
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	reader := &StandAloneStorageReader{}
	reader.txn = s.Kv.NewTransaction(false) // false means read-only transcation
	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	wb := &engine_util.WriteBatch{}
	for _, m := range batch {
		wb.SetCF(m.Cf(), m.Key(), m.Value())
	}
	var err error
	dberr := wb.WriteToDB(s.Kv)
	if dberr != nil {
		switch dberr {
		case badger.ErrKeyNotFound:
			err = storage.ErrKeyNotFound
		default:
			err = storage.ErrUnknown
			log.Errorf("WriteToDB failed, err:%v", err)
		}
	}

	return err
}

func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	var err error
	val, dberr := engine_util.GetCFFromTxn(r.txn, cf, key)
	if dberr != nil && dberr != badger.ErrKeyNotFound {
		err = storage.ErrUnknown
	}

	return val, err
}

func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *StandAloneStorageReader) Close() {
	r.txn.Discard()
}
