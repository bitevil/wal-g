package walg

import (
	"testing"
	"fmt"
	"io/ioutil"
	"os"
	"bytes"
	"log"
	"io"
)

const (
	pagedFileName        = "testdata/paged_file.bin"
	sampeLSN      uint64 = 0xc6bd4600
)

func TestCheckType(t *testing.T) {
	loclLSN := sampeLSN
	reader, isPaged, size, err := ReadDatabaseFile(pagedFileName, &loclLSN)
	file, _ := os.Stat(pagedFileName)
	if err != nil {
		fmt.Print(err.Error())
	}
	buf, _ := ioutil.ReadAll(reader)
	if !isPaged {
		t.Error("Sample file is paged")
	}

	if int64(len(buf)) >= file.Size() {
		t.Error("Increment is too big")
	}

	if int(size) != len(buf){
		t.Error("Increment has wrong size")
	}

	tmpFileName := pagedFileName + "_tmp"
	CopyFile(pagedFileName, tmpFileName)
	defer os.Remove(tmpFileName)

	tmpFile, _ := os.OpenFile(tmpFileName, os.O_RDWR, 0666)
	tmpFile.WriteAt(make([]byte, 12345), 477421568-12345)
	tmpFile.Close()

	newReader := bytes.NewReader(buf)
	err = ApplyFileIncrement(tmpFileName, newReader)
	if err != nil {
		t.Error(err)
	}

	_, err = newReader.Read(make([]byte, 1))
	if err!=io.EOF {
		t.Error("Not read to the end")
	}

	compare := deepCompare(pagedFileName, tmpFileName)
	if !compare {
		t.Error("Icrement could not restore file")
	}

}

func CopyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}
	return out.Close()
}

const chunkSize = 64

func deepCompare(file1, file2 string) bool {
	// Check file size ...

	f1, err := os.Open(file1)
	if err != nil {
		log.Fatal(err)
	}

	f2, err := os.Open(file2)
	if err != nil {
		log.Fatal(err)
	}
	var chunkNumber = 0
	for {

		b1 := make([]byte, chunkSize)
		_, err1 := f1.Read(b1)

		b2 := make([]byte, chunkSize)
		_, err2 := f2.Read(b2)

		if err1 != nil || err2 != nil {
			if err1 == io.EOF && err2 == io.EOF {
				return true
			} else if err1 == io.EOF || err2 == io.EOF {
				return false
			} else {
				log.Fatal(err1, err2)
			}
		}

		if !bytes.Equal(b1, b2) {
			log.Printf("Bytes at %v differ\n",chunkNumber * chunkSize)
			log.Println(b1)
			log.Println(b2)
			return false
		}
		chunkNumber++
	}
}
