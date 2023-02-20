package block

import (
	"Chain/pkg/pro"
	"crypto/sha256"
	"fmt"
	"google.golang.org/protobuf/proto"
)

// TransactionInput is used as the input to create a TransactionOutput.
// Recall that TransactionInputs generate TransactionOutputs which in turn
// generate new TransactionInputs and so forth.
// ReferenceTransactionHash is the hash of the parent TransactionOutput's Transaction.
// OutputIndex is the index of the parent TransactionOutput's Transaction.
// Signature verifies that the payer can spend the referenced TransactionOutput.
type TransactionInput struct {
	ReferenceTransactionHash string // the hash of the parent TransactionOutput's Transaction
	OutputIndex              uint32 // the index of the parent TransactionOutput's Transaction
	UnlockingScript          string // Signature, verifies that the payer can spend the referenced TransactionOutput
}

// TransactionOutput is an output created from a TransactionInput.
// Recall that TransactionOutputs generate TransactionInputs which in turn
// generate new TransactionOutputs and so forth.
// Amount is how much this TransactionOutput is worth.
// PublicKey is used to verify the payee's signature.
type TransactionOutput struct {
	Amount        uint32 // how much this TransactionOutput is worth
	LockingScript string // PublicKey, is used to verify the payee's signature
}

// Transaction contains information about a transaction.
// Version is the version of this transaction.
// Inputs is a slice of TransactionInputs.
// Outputs is a slice of TransactionOutputs.
// LockTime is the future time after which the Transaction is valid.
type Transaction struct {
	Version  uint32
	Inputs   []*TransactionInput
	Outputs  []*TransactionOutput
	LockTime uint32
}

// EncodeTransactionInput returns a pro.TransactionInput input
// given a TransactionInput.
func EncodeTransactionInput(txi *TransactionInput) *pro.TransactionInput {
	return &pro.TransactionInput{
		ReferenceTransactionHash: txi.ReferenceTransactionHash,
		OutputIndex:              txi.OutputIndex,
		UnlockingScript:          txi.UnlockingScript,
	}
}

// DecodeTransactionInput returns a TransactionInput given
// a pro.TransactionInput.
func DecodeTransactionInput(ptxi *pro.TransactionInput) *TransactionInput {
	return &TransactionInput{
		ReferenceTransactionHash: ptxi.GetReferenceTransactionHash(),
		OutputIndex:              ptxi.GetOutputIndex(),
		UnlockingScript:          ptxi.GetUnlockingScript(),
	}
}

// EncodeTransactionOutput returns a pro.TransactionOutput given
// a TransactionOutput.
func EncodeTransactionOutput(txo *TransactionOutput) *pro.TransactionOutput {
	return &pro.TransactionOutput{
		Amount:        txo.Amount,
		LockingScript: txo.LockingScript,
	}
}

// DecodeTransactionOutput returns a TransactionOutput given
// a pro.TransactionOutput.
func DecodeTransactionOutput(ptxo *pro.TransactionOutput) *TransactionOutput {
	return &TransactionOutput{
		Amount:        ptxo.GetAmount(),
		LockingScript: ptxo.GetLockingScript(),
	}
}

// EncodeTransaction returns a pro.Transaction given a Transaction.
func EncodeTransaction(tx *Transaction) *pro.Transaction {
	var ptxis []*pro.TransactionInput
	for _, txi := range tx.Inputs {
		ptxis = append(ptxis, EncodeTransactionInput(txi))
	}
	var ptxos []*pro.TransactionOutput
	for _, txo := range tx.Outputs {
		ptxos = append(ptxos, EncodeTransactionOutput(txo))
	}
	return &pro.Transaction{
		Version:  tx.Version,
		Inputs:   ptxis,
		Outputs:  ptxos,
		LockTime: tx.LockTime,
	}
}

// DecodeTransaction returns a Transaction given a pro.Transaction.
func DecodeTransaction(ptx *pro.Transaction) *Transaction {
	var txis []*TransactionInput
	for _, ptxi := range ptx.GetInputs() {
		txis = append(txis, DecodeTransactionInput(ptxi))
	}
	var txos []*TransactionOutput
	for _, ptxo := range ptx.GetOutputs() {
		txos = append(txos, DecodeTransactionOutput(ptxo))
	}
	return &Transaction{
		Version:  ptx.GetVersion(),
		Inputs:   txis,
		Outputs:  txos,
		LockTime: ptx.GetLockTime(),
	}
}

// Hash returns the hash of the transaction
func (tx *Transaction) Hash() string {
	h := sha256.New()
	pt := EncodeTransaction(tx)
	bytes, err := proto.Marshal(pt)
	if err != nil {
		fmt.Errorf("[tx.Hash()] Unable to marshal transaction")
	}
	h.Write(bytes)
	return fmt.Sprintf("%x", h.Sum(nil))
}
