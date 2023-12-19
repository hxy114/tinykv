package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		return nil, err
	}
	value, err2 := reader.GetCF(req.GetCf(), req.Key)
	if err2 != nil {
		return nil, err2
	}
	kv := &kvrpcpb.RawGetResponse{}
	if value == nil {
		kv.Value = nil
		kv.NotFound = true
	} else {
		kv.Value = value
		kv.NotFound = false
	}

	return kv, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified

	put := storage.Put{Cf: req.Cf,
		Key:   req.Key,
		Value: req.Value}
	modify := make([]storage.Modify, 0)
	modify = append(modify, storage.Modify{Data: put})
	err := server.storage.Write(req.Context, modify)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	delete := storage.Delete{Cf: req.Cf,
		Key: req.Key,
	}
	modify := make([]storage.Modify, 0)
	modify = append(modify, storage.Modify{Data: delete})
	err := server.storage.Write(req.Context, modify)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		return nil, err
	}
	iter := reader.IterCF(req.Cf)
	defer iter.Close()
	iter.Seek(req.StartKey)
	kv := &kvrpcpb.RawScanResponse{Kvs: make([]*kvrpcpb.KvPair, 0)}
	var i uint32 = 0
	for i = 0; i < req.Limit; i++ {
		if iter.Valid() {
			key := iter.Item().KeyCopy(nil)
			value, _ := iter.Item().ValueCopy(nil)
			kvPair := &kvrpcpb.KvPair{Key: key, Value: value}
			kv.Kvs = append(kv.Kvs, kvPair)
			iter.Next()
		} else {
			break
		}

	}
	return kv, nil
}
