// Package coindatabase stores all UTXO information. It contains a cache of Coins and a LevelDB of CoinRecords.
// A Coin is a wrapper for a UTXO, and a CoinRecord is a record of all the UTXO created by a Transaction.
// Validation needs to be as quick as possible, which is why the CoinDatabase contains an in-memory cache in addition to a persistent database.
// Eventually the cache becomes too large, at which point the CoinDatabase must flush its cache to its database.
package coindatabase

import (
	"Chain/pkg/block"
	"Chain/pkg/blockchain/chainwriter"
	"Chain/pkg/pro"
	"Chain/pkg/utils"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"google.golang.org/protobuf/proto"
)

// CoinDatabase keeps track of Coins.
// db is a levelDB for persistent storage.
// mainCache stores as many Coins as possible for rapid validation.
// mainCacheSize is how many Coins are currently in the mainCache.
// mainCacheCapacity is the maximum number of Coins that the mainCache
// can store before it must flush.
type CoinDatabase struct {
	db                *leveldb.DB
	MainCache         map[CoinLocator]*Coin // stores as many Coins as possible for rapid validation
	MainCacheSize     uint32                // number of Coins currently in the MainCache
	MainCacheCapacity uint32                // the maximum number of Coins that the MainCache can store before it must flush
}

// New returns a CoinDatabase given a Config.
func New(config *Config) *CoinDatabase {
	db, err := leveldb.OpenFile(config.DatabasePath, nil)
	if err != nil {
		utils.Debug.Printf("Unable to initialize BlockInfoDatabase with path {%v}", config.DatabasePath)
	}
	return &CoinDatabase{
		db:                db,
		MainCache:         make(map[CoinLocator]*Coin),
		MainCacheSize:     0,
		MainCacheCapacity: config.MainCacheCapacity,
	}
}

// ValidateBlock returns whether a Block's Transactions are valid.
func (coinDB *CoinDatabase) ValidateBlock(transactions []*block.Transaction) bool {
	for _, tx := range transactions {
		if err := coinDB.validateTransaction(tx); err != nil {
			utils.Debug.Printf("%v", err)
			return false
		}
	}
	return true
}

// validateTransaction checks whether a Transaction's inputs are valid Coins.
// If the Coins have already been spent or do not exist, validateTransaction
// returns an error.
func (coinDB *CoinDatabase) validateTransaction(transaction *block.Transaction) error {
	for _, txi := range transaction.Inputs {
		key := makeCoinLocator(txi)
		if coin, ok := coinDB.MainCache[key]; ok {
			if coin.IsSpent {
				return fmt.Errorf("[validateTransaction] coin already spent")
			}
			continue
		}
		if data, err := coinDB.db.Get([]byte(txi.ReferenceTransactionHash), nil); err != nil {
			return fmt.Errorf("[validateTransaction] coin not in leveldb")
		} else {
			pcr := &pro.CoinRecord{}
			if err2 := proto.Unmarshal(data, pcr); err2 != nil {
				utils.Debug.Printf("Failed to unmarshal record from hash {%v}:", txi.ReferenceTransactionHash, err)
			}
			cr := DecodeCoinRecord(pcr)
			if !contains(cr.OutputIndexes, txi.OutputIndex) {
				return fmt.Errorf("[validateTransaction] coin record did not still contain output required for transaction input ")
			}
		}
	}
	return nil
}

// UndoCoins handles reverting a Block. It:
//
//	(1) erases the Coins created by a Block
//	(2) marks the Coins used to create those Transactions as unspent.
//
// Block inputs are in reversed order. https://edstem.org/us/courses/36337/discussion/2578832
func (coinDB *CoinDatabase) UndoCoins(blocks []*block.Block, undoBlocks []*chainwriter.UndoBlock) {
	for i := 0; i < len(blocks); i++ {
		for _, tx := range blocks[i].Transactions {
			coinDB.removeCreatedCoins(tx)
		}
		coinDB.markCoinsUnspent(undoBlocks[i])
	}
}

// addCoinToRecord adds a Coin to a CoinRecord given an UndoBlock and index,
// returning the updated CoinRecord.
func (coinDB *CoinDatabase) addCoinToRecord(cr *CoinRecord, ub *chainwriter.UndoBlock, index int) *CoinRecord {
	cr.OutputIndexes = append(cr.OutputIndexes, ub.OutputIndexes[index])
	cr.Amounts = append(cr.Amounts, ub.Amounts[index])
	cr.LockingScripts = append(cr.LockingScripts, ub.LockingScripts[index])
	return cr
}

// FlushMainCache flushes the mainCache to the db.
func (coinDB *CoinDatabase) FlushMainCache() {
	// update coin records
	updatedCoinRecords := make(map[string]*CoinRecord)
	for cl := range coinDB.MainCache {
		// check whether we already updated this record
		var cr *CoinRecord

		// (1) get our coin record
		// first check our map, in case we already updated the coin record given
		// a previous coin
		if cr2, ok := updatedCoinRecords[cl.ReferenceTransactionHash]; ok {
			cr = cr2
		} else {
			// if we haven't already update this coin record, retrieve from db
			data, err := coinDB.db.Get([]byte(cl.ReferenceTransactionHash), nil)
			if err != nil {
				utils.Debug.Printf("[FlushMainCache] coin record not in leveldb")
			}
			pcr := &pro.CoinRecord{}
			if err = proto.Unmarshal(data, pcr); err != nil {
				utils.Debug.Printf("Failed to unmarshal record from hash {%v}:%v", cl.ReferenceTransactionHash, err)
			}
			cr = DecodeCoinRecord(pcr)
		}
		// (2) remove the coin from the record if it's been spent
		if coinDB.MainCache[cl].IsSpent {
			cr = coinDB.removeCoinFromRecord(cr, cl.OutputIndex)
		}
		updatedCoinRecords[cl.ReferenceTransactionHash] = cr
		delete(coinDB.MainCache, cl)
	}
	coinDB.MainCacheSize = 0
	// write the new records
	for key, cr := range updatedCoinRecords {
		if len(cr.OutputIndexes) == 0 {
			err := coinDB.db.Delete([]byte(key), nil)
			if err != nil {
				utils.Debug.Printf("[FlushMainCache] failed to delete key {%v}", key)
			}
		} else {
			coinDB.putRecordInDB(key, cr)
		}
	}
}

// StoreBlock handles storing a newly minted Block. It:
//
//	(1) removes spent TransactionOutputs
//	(2) stores new TransactionOutputs as Coins in the mainCache
//	(3) stores CoinRecords for the Transactions in the db.
//
// We recommend you write a helper function for each subtask.
func (coinDB *CoinDatabase) StoreBlock(transactions []*block.Transaction) {
	for _, tx := range transactions {
		coinDB.removeSpentCoins(tx)
		coinDB.storeTxOutInCache(tx)
		coinDB.writeCrToDatabase(tx)
	}
}

// removeCoinFromDB removes a Coin from a CoinRecord, deleting the CoinRecord
// from the db entirely if it is the last remaining Coin in the CoinRecord.
func (coinDB *CoinDatabase) removeCoinFromDB(txHash string, cl CoinLocator) {
	cr := coinDB.getCoinRecordFromDB(txHash)
	switch {
	case cr == nil:
		return
	case len(cr.Amounts) <= 1:
		if err := coinDB.db.Delete([]byte(txHash), nil); err != nil {
			utils.Debug.Printf("[removeCoinFromDB] failed to remove {%v} from db", txHash)
		}
	default:
		cr = coinDB.removeCoinFromRecord(cr, cl.OutputIndex)
		coinDB.putRecordInDB(txHash, cr)
	}
}

// putRecordInDB puts a CoinRecord into the db.
func (coinDB *CoinDatabase) putRecordInDB(txHash string, cr *CoinRecord) {
	record := EncodeCoinRecord(cr)
	bytes, err := proto.Marshal(record)
	if err != nil {
		utils.Debug.Printf("[coindatabase.putRecordInDB] Unable to marshal coin record for key {%v}", txHash)
	}
	if err2 := coinDB.db.Put([]byte(txHash), bytes, nil); err2 != nil {
		utils.Debug.Printf("Unable to store coin record for key {%v}", txHash)
	}
}

// removeCoinFromRecord returns an updated CoinRecord. It removes the Coin
// with the given outputIndex, if the Coin exists in the CoinRecord.
func (coinDB *CoinDatabase) removeCoinFromRecord(cr *CoinRecord, outputIndex uint32) *CoinRecord {
	index := indexOf(cr.OutputIndexes, outputIndex)
	if index < 0 {
		return cr
	}
	cr.OutputIndexes = append(cr.OutputIndexes[:index], cr.OutputIndexes[index+1:]...)
	cr.Amounts = append(cr.Amounts[:index], cr.Amounts[index+1:]...)
	cr.LockingScripts = append(cr.LockingScripts[:index], cr.LockingScripts[index+1:]...)
	return cr
}

// createCoinRecord returns a CoinRecord for the provided Transaction.
func (coinDB *CoinDatabase) createCoinRecord(tx *block.Transaction) *CoinRecord {
	var outputIndexes []uint32
	var amounts []uint32
	var LockingScripts []string
	for i, txo := range tx.Outputs {
		outputIndexes = append(outputIndexes, uint32(i))
		amounts = append(amounts, txo.Amount)
		LockingScripts = append(LockingScripts, txo.LockingScript)
	}
	cr := &CoinRecord{
		Version:        0,
		OutputIndexes:  outputIndexes,
		Amounts:        amounts,
		LockingScripts: LockingScripts,
	}
	return cr
}

// getCoinRecordFromDB returns a CoinRecord from the db given a hash.
func (coinDB *CoinDatabase) getCoinRecordFromDB(txHash string) *CoinRecord {
	if data, err := coinDB.db.Get([]byte(txHash), nil); err != nil {
		utils.Debug.Printf("[validateTransaction] coin not in leveldb")
		return nil
	} else {
		pcr := &pro.CoinRecord{}
		if err := proto.Unmarshal(data, pcr); err != nil {
			utils.Debug.Printf("Failed to unmarshal record from hash {%v}:", txHash, err)
		}
		cr := DecodeCoinRecord(pcr)
		return cr
	}
}

// GetCoin returns a Coin given a CoinLocator. It first checks the
// mainCache, then checks the db. If the Coin doesn't exist,
// it returns nil.
func (coinDB *CoinDatabase) GetCoin(cl CoinLocator) *Coin {
	if coin, ok := coinDB.MainCache[cl]; ok {
		return coin
	}
	cr := coinDB.getCoinRecordFromDB(cl.ReferenceTransactionHash)
	if cr == nil {
		return nil
	}
	index := indexOf(cr.OutputIndexes, cl.OutputIndex)
	if index < 0 {
		return nil
	}
	return &Coin{
		TransactionOutput: &block.TransactionOutput{
			Amount:        cr.Amounts[index],
			LockingScript: cr.LockingScripts[index],
		},
		IsSpent: false,
	}
}

// contains returns true if an int slice s contains element e, false if it does not.
func contains(s []uint32, e uint32) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

// indexOf returns the index of element e in int slice s, -1 if the element does not exist.
func indexOf(s []uint32, e uint32) int {
	for i, a := range s {
		if a == e {
			return i
		}
	}
	return -1
}

// helper for StoreBlock
func (coinDB *CoinDatabase) removeSpentCoins(tx *block.Transaction) {
	for _, input := range tx.Inputs {
		cl := makeCoinLocator(input)
		if coin, ok := coinDB.MainCache[cl]; ok {
			// coin is in mainCache
			coin.IsSpent = true
		} else if cr := coinDB.getCoinRecordFromDB(cl.ReferenceTransactionHash); cr != nil {
			// coin is in db
			coinDB.removeCoinFromDB(cl.ReferenceTransactionHash, cl)
		} else {
			utils.Debug.Printf("[removeSpentCoins] failed. Coin in transaction {%v} doesn't exist!\n", cl.ReferenceTransactionHash)
		}
	}
}

// helper for StoreBlock
func (coinDB *CoinDatabase) storeTxOutInCache(tx *block.Transaction) {
	for idx, output := range tx.Outputs {
		if coinDB.MainCacheSize >= coinDB.MainCacheCapacity {
			coinDB.FlushMainCache()
		}
		cl := CoinLocator{tx.Hash(), uint32(idx)}
		coin := &Coin{output, false}
		coinDB.MainCache[cl] = coin
		coinDB.MainCacheSize += 1
	}
}

// helper for StoreBlock
func (coinDB *CoinDatabase) writeCrToDatabase(tx *block.Transaction) {
	cr := coinDB.createCoinRecord(tx)
	coinDB.putRecordInDB(tx.Hash(), cr)
}

// helper for UndoCoins
func (coinDB *CoinDatabase) removeCreatedCoins(tx *block.Transaction) {
	for idx, _ := range tx.Outputs {
		cl := CoinLocator{tx.Hash(), uint32(idx)}
		// remove from database
		coinDB.removeCoinFromDB(cl.ReferenceTransactionHash, cl)
		// remove from cache
		delete(coinDB.MainCache, cl)
		coinDB.MainCacheSize -= 1
	}
}

// helper for UndoCoins
// https://edstem.org/us/courses/36337/discussion/2593635
func (coinDB *CoinDatabase) markCoinsUnspent(undoBlock *chainwriter.UndoBlock) {
	for i := 0; i < len(undoBlock.TransactionInputHashes); i++ {
		// get coinRecord from db -> add coin to coinRecord -> store to db
		cr := coinDB.getCoinRecordFromDB(undoBlock.TransactionInputHashes[i])
		coinDB.addCoinToRecord(cr, undoBlock, i)
		coinDB.putRecordInDB(undoBlock.TransactionInputHashes[i], cr)

		// if coin in mainCache -> mark as unspent
		cl := CoinLocator{undoBlock.TransactionInputHashes[i], undoBlock.OutputIndexes[i]}
		if coin, ok := coinDB.MainCache[cl]; ok {
			coin.IsSpent = false
		}
	}
}
