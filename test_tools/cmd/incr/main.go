package main

import (
	"github.com/wal-g/wal-g"
	"log"
	"os"
	"os/exec"
	"time"
	"fmt"
	"strconv"
	"path/filepath"
	"strings"
	"io"
	"bytes"
)

func RemoveContents(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {
		err = os.RemoveAll(filepath.Join(dir, name))
		if err != nil {
			return err
		}
	}
	return nil
}

const (
	baseDir        = "/Users/x4mmm/DemoDb"
	restoreDir     = "/Users/x4mmm/DemoDbRestore"
	pgbin          = "/Users/x4mmm/project/bin/"
	pgbenchCommand = pgbin + "pgbench"
	pgctlCommand   = pgbin + "pg_ctl"
	initdbCommand  = pgbin + "initdb"
)

func main() {
	os.Setenv("WALE_S3_PREFIX", os.Getenv("WALE_S3_PREFIX")+"/"+strconv.FormatInt(time.Now().UnixNano(), 10))
	tu, pre, err := walg.Configure()
	if err != nil {
		log.Fatal(err);
	}

	WipeRestore()

	Backup(tu, pre)
	Bench()
	time.Sleep(15000000000)
	Backup(tu, pre)

	Fetch(pre)

	Diff()
}

func WipeRestore() {
	err := RemoveContents(restoreDir)
	if err != nil {
		log.Fatal(err);
	}
	err = os.MkdirAll(restoreDir, 0777)
	if err != nil {
		log.Fatal(err);
	}
}

func Fetch(pre *walg.Prefix) {
	walg.HandleIncrementalFetch("LATEST", pre, restoreDir, false)
}

func Diff() {
	out, _ := exec.Command("diff", "-r", baseDir, restoreDir).Output()
	outStr := string(out)
	fmt.Println(outStr)
	if strings.Contains(outStr, "differ") {
		PrintDiff(strings.Split(outStr, "\n"))
		log.Fatal("diff output contains difference")
	}
}
func PrintDiff(rows []string) {
	for _, r := range rows {
		if !strings.Contains(r, "differ") {
			continue
		}

		r = strings.Split(r, "Binary files ")[1]
		r = strings.Split(r, " differ")[0]
		v := strings.Split(r, " and ")
		fmt.Println("File 1: " + v[0])
		fmt.Println("File 2: " + v[1])
		PagedFileCompare(v[0], v[1])
	}
}
func PagedFileCompare(filename1 string, filename2 string) {
	f1, _ := os.Open(filename1)
	f2, _ := os.Open(filename2)

	chunkSize := int(walg.BlockSize)

	var chunkNumber = 0
	for {

		b1 := make([]byte, chunkSize)
		_, err1 := f1.Read(b1)

		b2 := make([]byte, chunkSize)
		_, err2 := f2.Read(b2)

		if err1 != nil || err2 != nil {
			if err1 == io.EOF && err2 == io.EOF {
				return
			} else if err1 == io.EOF || err2 == io.EOF {
				fmt.Println("Files have different sizes")
			} else {
				log.Fatal(err1, err2)
			}
		}

		if !bytes.Equal(b1, b2) {
			fmt.Printf("Bytes at %x differ\n", chunkNumber*chunkSize)
			lsn1, valid1 := walg.ParsePageHeader(b1)
			fmt.Printf("LSN1 %x valid %v\n", lsn1, valid1)
			lsn2, valid2 := walg.ParsePageHeader(b2)
			fmt.Printf("LSN2 %x valid %v\n", lsn2, valid2)

			if lsn1!=lsn2 {
				log.Panic("Increment pages did not restore page with different LSN")
			}

			fmt.Println(b1)
			fmt.Println(b2)
			for x := 0; x < int(walg.BlockSize); x++ {
				if b1[x] != b2[x] {
					fmt.Printf("Different bytes %x and %x at %x\n", b1[x], b2[x], x)
				}
			}
		}
		chunkNumber++
	}
}

func Bench() {
	var err error
	out, err := exec.Command(pgbenchCommand, "postgres", "-T", "3").Output()
	fmt.Println(string(out))
	if err != nil {
		log.Fatal(err);
	}
}

func Ctl(command string) {
	go func() {
		var err error
		out, err := exec.Command(pgctlCommand, "-D", baseDir, command).Output()
		fmt.Println(string(out))
		if err != nil {
			log.Fatal(err);
		}
	}()
	time.Sleep(100) // peace of shell
}
func InitDb() {
	var err error
	out, err := exec.Command(initdbCommand, baseDir).Output()
	fmt.Println(string(out))
	if err != nil {
		log.Fatal(err);
	}
}

func SetupBench() {
	var err error
	out, err := exec.Command(pgbenchCommand, "postgres", "-i").Output()
	fmt.Println(string(out))
	if err != nil {
		fmt.Println(err);
	}
}
func Backup(tu *walg.TarUploader, pre *walg.Prefix) {
	walg.HandleIncrementalBackup(baseDir, tu, pre)
}
