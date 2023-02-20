package chainwriter

import "Chain/pkg/pro"

// UndoBlock is used to reverse the side effects causes by a Block.
// It contains information necessary for reverting a specific block.
// Thus, each Block has an accompanying UndoBlock.
// When the chain reverts a block's Transactions, it must both
//
//	(1) remove newly created TransactionOutputs, and
//	(2) convert TransactionInputs back into available TransactionOutputs.
//
// This struct helps with (2).
// TransactionInputHashes are the hashes of the TransactionInputs that
// the UndoBlock must revert.
// OutputIndexes are the OutputIndexes of the TransactionInputs.
// Amounts are the amounts of the parent TransactionOutputs.
// LockingScripts are the locking scripts of the parent TransactionOutputs.
type UndoBlock struct {
	TransactionInputHashes []string // the hashes of the TransactionInputs that the UndoBlock must revert
	OutputIndexes          []uint32 // the OutputIndexes of the TransactionInputs
	Amounts                []uint32 // the amounts of the parent TransactionOutputs
	LockingScripts         []string // the locking scripts of the parent TransactionOutputs
}

// EncodeUndoBlock returns a pro.UndoBlock given an UndoBlock.
func EncodeUndoBlock(ub *UndoBlock) *pro.UndoBlock {
	var transactionInputHashes []string
	var outputIndexes []uint32
	var amounts []uint32
	var lockingScripts []string
	for i := 0; i < len(ub.TransactionInputHashes); i++ {
		transactionInputHashes = append(transactionInputHashes, ub.TransactionInputHashes[i])
		outputIndexes = append(outputIndexes, ub.OutputIndexes[i])
		amounts = append(amounts, ub.Amounts[i])
		lockingScripts = append(lockingScripts, ub.LockingScripts[i])
	}
	return &pro.UndoBlock{
		TransactionInputHashes: transactionInputHashes,
		OutputIndexes:          outputIndexes,
		Amounts:                amounts,
		LockingScripts:         lockingScripts,
	}
}

// DecodeUndoBlock returns an UndoBlock given a pro.UndoBlock
func DecodeUndoBlock(pub *pro.UndoBlock) *UndoBlock {
	var transactionInputHashes []string
	var outputIndexes []uint32
	var amounts []uint32
	var lockingScripts []string
	for i := 0; i < len(pub.GetTransactionInputHashes()); i++ {
		transactionInputHashes = append(transactionInputHashes, pub.GetTransactionInputHashes()[i])
		outputIndexes = append(outputIndexes, pub.GetOutputIndexes()[i])
		amounts = append(amounts, pub.GetAmounts()[i])
		lockingScripts = append(lockingScripts, pub.GetLockingScripts()[i])
	}
	return &UndoBlock{
		TransactionInputHashes: transactionInputHashes,
		OutputIndexes:          outputIndexes,
		Amounts:                amounts,
		LockingScripts:         lockingScripts,
	}
}
