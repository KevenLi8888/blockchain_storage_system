// Package blockinfodatabase is a wrapper for a LevelDB,
// storing information about each Block it receives in the form of a BlockRecord.
// Key - hash(Block), Value - BlockRecord (serialized with protocol buffer)
// In addition, each BlockRecord contains storage information for an UndoBlock,
// which provides additional information to revert a Block, should a fork occur.
package blockinfodatabase

import (
	"Chain/pkg/pro"
	"Chain/pkg/utils"
	"google.golang.org/protobuf/proto"

	"github.com/syndtr/goleveldb/leveldb"
)

// BlockInfoDatabase is a wrapper for a levelDB
type BlockInfoDatabase struct {
	db *leveldb.DB
}

// New returns a BlockInfoDatabase given a Config
func New(config *Config) *BlockInfoDatabase {
	db, err := leveldb.OpenFile(config.DatabasePath, nil)
	if err != nil {
		utils.Debug.Printf("Unable to initialize BlockInfoDatabase with path {%v}", config.DatabasePath)
	}
	return &BlockInfoDatabase{db: db}
}

// StoreBlockRecord stores a block record in the block info database.
//
//  1. encode the BlockRecord as a protobuf
//  2. convert the protobuf to the correct format and type (byte[]) so that it can be inserted into the database
//  3. put the block record into the database
func (blockInfoDB *BlockInfoDatabase) StoreBlockRecord(hash string, blockRecord *BlockRecord) {
	encodedBlock := EncodeBlockRecord(blockRecord)
	// https://protobuf.dev/getting-started/gotutorial/#writing-a-message
	serialized, err := proto.Marshal(encodedBlock)
	if err != nil {
		utils.Debug.Println("Failed to serialize block record: ", err)
	}
	if err := blockInfoDB.db.Put([]byte(hash), serialized, nil); err != nil {
		utils.Debug.Println("Failed to store block record to block info database: ", err)
	}
}

// GetBlockRecord returns a BlockRecord from the BlockInfoDatabase given
// the relevant block's hash.
//
//  1. retrieve the block record from the database
//  2. Convert the byte[] returned by the database to a protobuf
//  3. convert the protobuf back into a BlockRecord
func (blockInfoDB *BlockInfoDatabase) GetBlockRecord(hash string) *BlockRecord {
	data, err := blockInfoDB.db.Get([]byte(hash), nil)
	if err != nil {
		utils.Debug.Println("Failed to retrieve the block record from database: ", err)
	}
	// https://protobuf.dev/getting-started/gotutorial/#reading-a-message
	deserializedBlock := &pro.BlockRecord{}
	if err := proto.Unmarshal(data, deserializedBlock); err != nil {
		utils.Debug.Println("Failed to deserialize block record: ", err)
	}
	decodedBlock := DecodeBlockRecord(deserializedBlock)
	return decodedBlock
}
