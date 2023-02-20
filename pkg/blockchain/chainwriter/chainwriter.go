// Package chainwriter takes care of all I/O for the BlockChain. It writes and reads Blocks and UndoBlocks to Disk.
package chainwriter

import (
	"Chain/pkg/block"
	"Chain/pkg/blockchain/blockinfodatabase"
	"Chain/pkg/pro"
	"Chain/pkg/utils"
	"fmt"
	"log"
	"os"

	"google.golang.org/protobuf/proto"
)

// ChainWriter handles all I/O for the BlockChain. It stores and retrieves
// Blocks and UndoBlocks.
// See config.go for more information on its fields.
// Block files are of the format:
// "DataDirectory/BlockFileName_CurrentBlockFileNumber.FileExtension"
// Ex: "data/block_0.txt"
// UndoBlock files are of the format:
// "DataDirectory/UndoFileName_CurrentUndoFileNumber.FileExtension"
// Ex: "data/undo_0.txt"
type ChainWriter struct {
	// data storage information
	FileExtension string
	DataDirectory string

	// block information
	BlockFileName          string
	CurrentBlockFileNumber uint32
	CurrentBlockOffset     uint32
	MaxBlockFileSize       uint32

	// undo block information
	UndoFileName          string
	CurrentUndoFileNumber uint32
	CurrentUndoOffset     uint32
	MaxUndoFileSize       uint32
}

// New returns a ChainWriter given a Config.
func New(config *Config) *ChainWriter {
	if err := os.MkdirAll(config.DataDirectory, 0700); err != nil {
		log.Fatalf("Could not create ChainWriter's data directory")
	}
	return &ChainWriter{
		FileExtension:          config.FileExtension,
		DataDirectory:          config.DataDirectory,
		BlockFileName:          config.BlockFileName,
		CurrentBlockFileNumber: 0,
		CurrentBlockOffset:     0,
		MaxBlockFileSize:       config.MaxBlockFileSize,
		UndoFileName:           config.UndoFileName,
		CurrentUndoFileNumber:  0,
		CurrentUndoOffset:      0,
		MaxUndoFileSize:        config.MaxUndoFileSize,
	}
}

// StoreBlock stores a Block and its corresponding UndoBlock to Disk,
// returning a BlockRecord that contains information for later retrieval.
func (cw *ChainWriter) StoreBlock(bl *block.Block, undoBlock *UndoBlock, height uint32) *blockinfodatabase.BlockRecord {
	// serialize block
	b := block.EncodeBlock(bl)
	serializedBlock, err := proto.Marshal(b)
	if err != nil {
		utils.Debug.Printf("Failed to marshal block")
	}
	// serialize undo block
	ub := EncodeUndoBlock(undoBlock)
	serializedUndoBlock, err := proto.Marshal(ub)
	if err != nil {
		utils.Debug.Printf("Failed to marshal undo block")
	}
	// write block to disk
	bfi := cw.WriteBlock(serializedBlock)
	// create an empty file info, which we will update if the function is passed an undo block.
	ufi := &FileInfo{}
	if undoBlock.Amounts != nil {
		ufi = cw.WriteUndoBlock(serializedUndoBlock)
	}

	return &blockinfodatabase.BlockRecord{
		Header:               bl.Header,
		Height:               height,
		NumberOfTransactions: uint32(len(bl.Transactions)),
		BlockFile:            bfi.FileName,
		BlockStartOffset:     bfi.StartOffset,
		BlockEndOffset:       bfi.EndOffset,
		UndoFile:             ufi.FileName,
		UndoStartOffset:      ufi.StartOffset,
		UndoEndOffset:        ufi.EndOffset,
	}
}

// WriteBlock writes a serialized Block to Disk and returns
// a FileInfo for storage information.
func (cw *ChainWriter) WriteBlock(serializedBlock []byte) *FileInfo {
	// Before writing a block to a file, check that doing so will not cause the file to be larger than the maximum allowable file size.
	// If your Block/UndoBlock is too large to store in the current file, you’ll have to update where you’re writing to!
	blockSize := uint32(len(serializedBlock))
	if blockSize+cw.CurrentBlockOffset > cw.MaxBlockFileSize {
		cw.CurrentBlockFileNumber += 1
		cw.CurrentBlockOffset = 0
	}
	// https://stackoverflow.com/questions/11123865/format-a-go-string-without-printing
	// dataDirectory/fileName_fileNumber.<file extension>
	fileName := fmt.Sprintf("%v/%v_%v%v", cw.DataDirectory, cw.BlockFileName, cw.CurrentBlockFileNumber, cw.FileExtension)
	writeToDisk(fileName, serializedBlock)
	cw.CurrentBlockOffset += blockSize
	return &FileInfo{fileName, cw.CurrentBlockOffset - blockSize, cw.CurrentBlockOffset}
}

// WriteUndoBlock writes a serialized UndoBlock to Disk and returns
// a FileInfo for storage information.
func (cw *ChainWriter) WriteUndoBlock(serializedUndoBlock []byte) *FileInfo {
	// Similar to WriteBlock
	blockSize := uint32(len(serializedUndoBlock))
	if blockSize+cw.CurrentUndoOffset > cw.MaxUndoFileSize {
		cw.CurrentUndoFileNumber += 1
		cw.CurrentUndoOffset = 0
	}
	fileName := fmt.Sprintf("%v/%v_%v%v", cw.DataDirectory, cw.UndoFileName, cw.CurrentUndoFileNumber, cw.FileExtension)
	writeToDisk(fileName, serializedUndoBlock)
	cw.CurrentUndoOffset += blockSize
	return &FileInfo{fileName, cw.CurrentUndoOffset - blockSize, cw.CurrentUndoOffset}
}

// ReadBlock returns a Block given a FileInfo.
func (cw *ChainWriter) ReadBlock(fi *FileInfo) *block.Block {
	bytes := readFromDisk(fi)
	pb := &pro.Block{}
	if err := proto.Unmarshal(bytes, pb); err != nil {
		utils.Debug.Printf("failed to unmarshal block from file info {%v}", fi)
	}
	return block.DecodeBlock(pb)
}

// ReadUndoBlock returns an UndoBlock given a FileInfo.
func (cw *ChainWriter) ReadUndoBlock(fi *FileInfo) *UndoBlock {
	bytes := readFromDisk(fi)
	pub := &pro.UndoBlock{}
	if err := proto.Unmarshal(bytes, pub); err != nil {
		utils.Debug.Printf("failed to unmarshal undo block from file info {%v}", fi)
	}
	return DecodeUndoBlock(pub)
}
