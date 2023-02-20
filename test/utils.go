package test

import (
	"Chain/pkg/block"
	"Chain/pkg/blockchain/blockinfodatabase"
	"Chain/pkg/blockchain/chainwriter"
	"fmt"
	"os"
)

// cleanUp removes any directories created by a test.
func cleanUp() {
	removeBlockInfoDB()
	removeCoinDB()
	removeDataDB()
}

// removeCoinDB removes the coin database's level db.
func removeCoinDB() {
	if _, err := os.Stat("coindata"); !os.IsNotExist(err) {
		if err2 := os.RemoveAll("coindata"); err2 != nil {
			fmt.Errorf("could not remove leveldb coindata")
		}
	}
}

// removeBlockInfoDB removes the block info database's level db.
func removeBlockInfoDB() {
	if _, err := os.Stat("blockinfodata"); !os.IsNotExist(err) {
		if err2 := os.RemoveAll("blockinfodata"); err2 != nil {
			fmt.Errorf("could not remove leveldb blockinfodata")
		}
	}
}

//removeDataDB removes the chain writer's data directory.
func removeDataDB() {
	if _, err := os.Stat("data"); !os.IsNotExist(err) {
		if err2 := os.RemoveAll("data"); err2 != nil {
			fmt.Errorf("could not remove directory data")
		}
	}
}

// MockedHeader returns a mocked Header.
func MockedHeader() *block.Header {
	return &block.Header{
		Version:          0,
		PreviousHash:     "",
		MerkleRoot:       "",
		DifficultyTarget: "",
		Nonce:            0,
		Timestamp:        0,
	}
}

// MockedBlockRecord returns a mocked BlockRecord.
func MockedBlockRecord() *blockinfodatabase.BlockRecord {
	return &blockinfodatabase.BlockRecord{
		Header:               MockedHeader(),
		Height:               0,
		NumberOfTransactions: 0,
		BlockFile:            "blockinfodata/block_0",
		BlockStartOffset:     0,
		BlockEndOffset:       10,
		UndoFile:             "",
		UndoStartOffset:      0,
		UndoEndOffset:        0,
	}
}

// MockedTransactionInput returns a mocked TransactionInput.
func MockedTransactionInput() *block.TransactionInput {
	return &block.TransactionInput{
		ReferenceTransactionHash: "",
		OutputIndex:              0,
		UnlockingScript:          "",
	}
}

// MockedTransactionOutput returns a mocked TransactionOutput.
func MockedTransactionOutput() *block.TransactionOutput {
	return &block.TransactionOutput{
		Amount:        0,
		LockingScript: "",
	}
}

// MockedBlock returns a mocked Transaction.
func MockedTransaction() *block.Transaction {
	return &block.Transaction{
		Version:  0,
		Inputs:   []*block.TransactionInput{MockedTransactionInput()},
		Outputs:  []*block.TransactionOutput{MockedTransactionOutput()},
		LockTime: 0,
	}
}

// MockedBlock returns a mocked Block.
func MockedBlock() *block.Block {
	return &block.Block{
		Header:       MockedHeader(),
		Transactions: []*block.Transaction{MockedTransaction()},
	}
}

// MockedUndoBlock returns a mocked UndoBlock.
func MockedUndoBlock() *chainwriter.UndoBlock {
	return &chainwriter.UndoBlock{
		TransactionInputHashes: []string{""},
		OutputIndexes:          []uint32{1},
		Amounts:                []uint32{1},
		LockingScripts:         []string{""},
	}
}

//GenesisBlock returns the genesis block for testing purposes.
func GenesisBlock() *block.Block {
	txo := &block.TransactionOutput{
		Amount:        1_000_000_000,
		LockingScript: "pubkey",
	}
	genTx := &block.Transaction{
		Version:  0,
		Inputs:   nil,
		Outputs:  []*block.TransactionOutput{txo},
		LockTime: 0,
	}
	return &block.Block{
		Header: &block.Header{
			Version:          0,
			PreviousHash:     "",
			MerkleRoot:       "",
			DifficultyTarget: "",
			Nonce:            0,
			Timestamp:        0,
		},
		Transactions: []*block.Transaction{genTx},
	}
}

// MakeBlockFromPrev creates a new Block from an existing Block,
// using the old Block's TransactionOutputs as TransactionInputs
// for the new Transaction.
func MakeBlockFromPrev(b *block.Block) *block.Block {
	newHeader := &block.Header{
		Version:          0,
		PreviousHash:     b.Hash(),
		MerkleRoot:       "",
		DifficultyTarget: "",
		Nonce:            0,
		Timestamp:        0,
	}

	var transactions []*block.Transaction

	for _, tx := range b.Transactions {
		txHash := tx.Hash()
		for i, txo := range tx.Outputs {
			txi := &block.TransactionInput{
				ReferenceTransactionHash: txHash,
				OutputIndex:              uint32(i),
				UnlockingScript:          "",
			}
			txo1 := &block.TransactionOutput{
				Amount:        txo.Amount / 2,
				LockingScript: "",
			}
			tx1 := &block.Transaction{
				Version:  uint32(i),
				Inputs:   []*block.TransactionInput{txi},
				Outputs:  []*block.TransactionOutput{txo1},
				LockTime: 0,
			}
			transactions = append(transactions, tx1)
		}
	}
	return &block.Block{
		Header:       newHeader,
		Transactions: transactions,
	}
}

// UndoBlockFromBlock creates an UndoBlock from a Block.
// This function only works because we're not using inputs from
// other Blocks. It also does not actually take care of amounts
// or public keys.
func UndoBlockFromBlock(b *block.Block) *chainwriter.UndoBlock {
	var transactionHashes []string
	var outputIndexes []uint32
	var amounts []uint32
	var lockingScripts []string
	for _, tx := range b.Transactions {
		for _, txi := range tx.Inputs {
			transactionHashes = append(transactionHashes, txi.ReferenceTransactionHash)
			outputIndexes = append(outputIndexes, txi.OutputIndex)
			amounts = append(amounts, 0)
			lockingScripts = append(lockingScripts, "")
		}
	}
	return &chainwriter.UndoBlock{
		TransactionInputHashes: transactionHashes,
		OutputIndexes:          outputIndexes,
		Amounts:                amounts,
		LockingScripts:         lockingScripts,
	}
}
