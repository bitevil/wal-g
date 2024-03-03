package postgres

import (
	"encoding/gob"
	"fmt"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/utility"
	"io"
	"io/fs"
	"net"
	"os"
	"path"
	"path/filepath"
	"time"
)

func HandleCatchupSend(pgDataDirectory string, destination string) {
	pgDataDirectory = utility.ResolveSymlink(pgDataDirectory)
	tracelog.InfoLogger.Printf("Sending %v to %v\n", pgDataDirectory, destination)
	info, runner, err := GetPgServerInfo(true)
	if info.systemIdentifier == nil {
		tracelog.ErrorLogger.Fatal("Our system lacks System Identifier, cannot proceed")
	}
	tracelog.ErrorLogger.PanicOnError(err)
	dial, err := net.Dial("tcp", destination)
	tracelog.ErrorLogger.PanicOnError(err)
	decoder := gob.NewDecoder(dial)
	var control PgControlData
	err = decoder.Decode(&control)
	tracelog.ErrorLogger.PanicOnError(err)
	tracelog.InfoLogger.Printf("Destination control file %v", control)
	tracelog.InfoLogger.Printf("Our system id %v", *info.systemIdentifier)
	if *info.systemIdentifier != control.SystemIdentifier {
		tracelog.ErrorLogger.Fatal("System identifiers do not match")
	}
	if control.CurrentTimeline != info.Timeline {
		tracelog.ErrorLogger.Fatal("Destination is on timeline %v, but we are on %v", control.CurrentTimeline, info.Timeline)
	}
	var fileList internal.BackupFileList
	err = decoder.Decode(&fileList)
	tracelog.ErrorLogger.PanicOnError(err)
	tracelog.InfoLogger.Printf("Received file list of %v failes", len(fileList))
	_, lsnStr, _, err := runner.StartBackup("")
	tracelog.ErrorLogger.PanicOnError(err)
	lsn, err := ParseLSN(lsnStr)
	tracelog.ErrorLogger.PanicOnError(err)
	if lsn <= control.Checkpoint {
		tracelog.ErrorLogger.Fatal("Catchup destination is already ahead (our LSN %v, destination LSN %v).", lsn, control.Checkpoint)
	}

	encoder := gob.NewEncoder(dial)

	label, offsetMap, _, err := runner.StopBackup()
	tracelog.ErrorLogger.PanicOnError(err)

	sendFileCommand(dial, encoder, pgDataDirectory, fileList, control.Checkpoint)

	err = encoder.Encode(CatchupCommandDto{BinaryContents: []byte(label), FileName: BackupLabelFilename, IsBinContents: true})
	tracelog.ErrorLogger.PanicOnError(err)
	err = encoder.Encode(CatchupCommandDto{BinaryContents: []byte(offsetMap), FileName: TablespaceMapFilename, IsBinContents: true})
	tracelog.ErrorLogger.PanicOnError(err)
	ourPgControl, err := os.ReadFile(path.Join(pgDataDirectory, PgControlPath))
	tracelog.ErrorLogger.PanicOnError(err)
	err = encoder.Encode(CatchupCommandDto{BinaryContents: ourPgControl, FileName: utility.SanitizePath(PgControlPath), IsBinContents: true})
	tracelog.ErrorLogger.PanicOnError(err)

	err = encoder.Encode(CatchupCommandDto{IsDone: true})
	tracelog.ErrorLogger.PanicOnError(err)
	tracelog.InfoLogger.Printf("Send done")
}

func sendFileCommand(dial net.Conn, encoder *gob.Encoder, directory string, list internal.BackupFileList, checkpoint LSN) {
	filepath.Walk(directory, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				tracelog.WarningLogger.Println(path, " deleted during filepath walk")
				return nil
			}
			return err
		}
		if info.Name() == PgControl {
			return nil
		}
		fileName := info.Name()
		_, excluded := ExcludedFilenames[fileName]
		isDir := info.IsDir()
		if isDir && excluded {
			return filepath.SkipDir
		}
		if isDir {
			return nil
		}
		if excluded {
			return nil
		}
		fullFileName := utility.GetSubdirectoryRelativePath(path, directory)

		if fdto, ok := list[fullFileName]; ok {
			if fdto.MTime.Equal(info.ModTime()) {
				// No need to catchup
				return nil
			}
		}

		fd, err := os.Open(path)
		tracelog.ErrorLogger.PanicOnError(err)
		size := info.Size()
		err = encoder.Encode(CatchupCommandDto{FileName: fullFileName, IsFull: true, FileSize: uint64(size)})
		tracelog.ErrorLogger.PanicOnError(err)
		reader := &io.LimitedReader{
			R: io.MultiReader(fd, &ioextensions.ZeroReader{}),
			N: size,
		}

		n, err := utility.FastCopy(dial, reader)
		tracelog.InfoLogger.Printf("Sent %v, %v bytes", fullFileName, n)
		if n != size {
			tracelog.ErrorLogger.Fatalf("Short write %v instead of %v", n, size)
		}
		tracelog.ErrorLogger.PanicOnError(err)
		err = fd.Close()
		tracelog.ErrorLogger.PanicOnError(err)

		time.Sleep(time.Second)

		return nil
	})
	tracelog.DebugLogger.Printf("Filepath walk done")
}

func HandleCatchupReceive(pgDataDirectory string, port int) {
	pgDataDirectory = utility.ResolveSymlink(pgDataDirectory)
	tracelog.InfoLogger.Printf("Receiving %v on port %v\n", pgDataDirectory, port)
	listen, err := net.Listen("tcp", fmt.Sprintf(":%v", port))
	tracelog.ErrorLogger.PanicOnError(err)
	conn, err := listen.Accept()
	sendControlAndFileList(pgDataDirectory, err, conn)
	decoder := gob.NewDecoder(conn)
	for {
		var cmd CatchupCommandDto
		err := decoder.Decode(&cmd)
		if io.EOF == err {
			break
		}
		tracelog.ErrorLogger.PanicOnError(err)
		if cmd.IsDone {
			break
		}
		doRcvCommand(cmd, pgDataDirectory, conn)
	}
	tracelog.InfoLogger.Printf("Receive done")
}

func doRcvCommand(cmd CatchupCommandDto, directory string, conn net.Conn) {
	if cmd.IsBinContents {
		tracelog.InfoLogger.Printf("Writing file %v", cmd.FileName)
		err := os.WriteFile(path.Join(directory, cmd.FileName), cmd.BinaryContents, 0666)
		tracelog.ErrorLogger.PanicOnError(err)
		return
	}

	if cmd.IsFull {
		tracelog.InfoLogger.Printf("Full file %v", cmd.FileName)
		fd, err := os.Create(path.Join(directory, cmd.FileName))
		tracelog.ErrorLogger.PanicOnError(err)
		size := int64(cmd.FileSize)
		n, err := utility.FastCopy(fd, &io.LimitedReader{R: conn, N: size})
		if n != size {
			tracelog.InfoLogger.Printf("Received %v bytes instead of %v", n, size)
		}
		tracelog.InfoLogger.Printf("Received %v bytes", n)
		tracelog.ErrorLogger.PanicOnError(err)
		err = fd.Close()
		tracelog.ErrorLogger.PanicOnError(err)
	}
}

type CatchupCommandDto struct {
	IsDone         bool
	IsIncremental  bool
	IsFull         bool
	IsDelete       bool
	IsBinContents  bool
	FileSize       uint64
	FileName       string
	BinaryContents []byte
}

func sendControlAndFileList(pgDataDirectory string, err error, conn net.Conn) {
	tracelog.ErrorLogger.PanicOnError(err)
	control, err := ExtractPgControl(pgDataDirectory)
	tracelog.InfoLogger.Printf("Our system id %v, need catchup from %v", control.SystemIdentifier, control.Checkpoint)
	tracelog.ErrorLogger.PanicOnError(err)
	encoder := gob.NewEncoder(conn)
	err = encoder.Encode(control)
	tracelog.ErrorLogger.PanicOnError(err)
	rcvFileList := receiveFileList(pgDataDirectory)
	err = encoder.Encode(rcvFileList)
	tracelog.ErrorLogger.PanicOnError(err)
}

func receiveFileList(directory string) internal.BackupFileList {
	var result = make(internal.BackupFileList)
	filepath.Walk(directory, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			tracelog.WarningLogger.Println("Apparent concurrent modification")
			return err
		}
		if info.Name() == PgControl {
			return nil
		}
		fileName := info.Name()
		_, excluded := ExcludedFilenames[fileName]
		isDir := info.IsDir()
		if isDir && excluded {
			return filepath.SkipDir
		}
		if excluded {
			return nil
		}
		result[utility.GetSubdirectoryRelativePath(path, directory)] = internal.BackupFileDescription{MTime: info.ModTime(), IsSkipped: false, IsIncremented: false}

		return nil
	})
	return result
}
