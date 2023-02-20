package chainwriter

import (
	"log"
	"os"
)

// writeToDisk appends a slice of bytes to a file.
func writeToDisk(fileName string, data []byte) {
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Unable to open file {%v}", fileName)
	}
	if _, err := file.Write(data); err != nil {
		file.Close() // ignore error; Write error takes precedence
		log.Fatalf("Failed to write to file {%v}", fileName)
	}
	if err := file.Close(); err != nil {
		log.Fatalf("Failed to close file {%v}", fileName)
	}
}

// readFromDisk return a slice of bytes from a file, given a FileInfo.
func readFromDisk(info *FileInfo) []byte {
	file, err := os.Open(info.FileName)
	if err != nil {
		log.Fatalf("Unable to open file {%v}", info.FileName)
	}
	if _, err2 := file.Seek(int64(info.StartOffset), 0); err2 != nil {
		log.Fatalf("Failed to seek to {%v} in file {%v}", info.StartOffset, info.FileName)
	}
	numBytes := info.EndOffset - info.StartOffset
	buf := make([]byte, numBytes)
	if n, err3 := file.Read(buf); uint32(n) != info.EndOffset-info.StartOffset || err3 != nil {
		log.Fatalf("Failed to read {%v} bytes from file {%v}", numBytes, info.FileName)
	}
	if err4 := file.Close(); err4 != nil {
		log.Fatalf("Failed to close file {%v}", info.FileName)
	}
	return buf
}
