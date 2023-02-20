package block

import (
	"Chain/pkg/pro"
	"crypto/sha256"
	"fmt"
	"google.golang.org/protobuf/proto"
)

// Header provides information about the Block.
// Version is the Block's version.
// PreviousHash is the hash of the previous Block.
// MerkleRoot is the hash of all the Block's Transactions.
// DifficultyTarget is the difficulty of achieving a winning Nonce.
// Nonce is a "number only used once" that satisfies the DifficultyTarget.
// Timestamp is when the Block was successfully mined.
type Header struct {
	Version          uint32
	PreviousHash     string
	MerkleRoot       string
	DifficultyTarget string
	Nonce            uint32
	Timestamp        uint32
}

// Block includes a Header and a slice of Transactions.
type Block struct {
	Header       *Header
	Transactions []*Transaction
}

// EncodeHeader returns a pro.Header given a Header.
func EncodeHeader(header *Header) *pro.Header {
	return &pro.Header{
		Version:          header.Version,
		PreviousHash:     header.PreviousHash,
		MerkleRoot:       header.MerkleRoot,
		DifficultyTarget: header.DifficultyTarget,
		Nonce:            header.Nonce,
		Timestamp:        header.Timestamp,
	}
}

// DecodeHeader returns a Header given a pro.Header.
func DecodeHeader(pheader *pro.Header) *Header {
	return &Header{
		Version:          pheader.GetVersion(),
		PreviousHash:     pheader.GetPreviousHash(),
		MerkleRoot:       pheader.GetMerkleRoot(),
		DifficultyTarget: pheader.GetDifficultyTarget(),
		Nonce:            pheader.GetNonce(),
		Timestamp:        pheader.GetTimestamp(),
	}
}

// EncodeBlock returns a pro.Block given a Block.
func EncodeBlock(b *Block) *pro.Block {
	var ptxs []*pro.Transaction
	for _, tx := range b.Transactions {
		ptxs = append(ptxs, EncodeTransaction(tx))
	}
	return &pro.Block{
		Header:       EncodeHeader(b.Header),
		Transactions: ptxs,
	}
}

// DecodeBlock returns a Block given a pro.Block.
func DecodeBlock(pb *pro.Block) *Block {
	var txs []*Transaction
	for _, ptx := range pb.GetTransactions() {
		txs = append(txs, DecodeTransaction(ptx))
	}
	return &Block{
		Header:       DecodeHeader(pb.GetHeader()),
		Transactions: txs,
	}
}

// Hash returns the hash of the block (which is done via the header)
func (block *Block) Hash() string {
	h := sha256.New()
	pb := EncodeHeader(block.Header)
	bytes, err := proto.Marshal(pb)
	if err != nil {
		fmt.Errorf("[block.Hash()] Unable to marshal block")
	}
	h.Write(bytes)
	return fmt.Sprintf("%x", h.Sum(nil))
}
