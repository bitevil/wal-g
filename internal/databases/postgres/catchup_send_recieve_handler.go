package postgres

import (
	"encoding/gob"
	"fmt"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
	"io/fs"
	"net"
	"os"
	"path"
	"path/filepath"
)

func HandleCatchupSend(pgDataDirectory string, destination string) {
	pgDataDirectory = utility.ResolveSymlink(pgDataDirectory)
	tracelog.InfoLogger.Printf("Sending %v to %v\n", pgDataDirectory, destination)
	info, runner, err := GetPgServerInfo(true)
	if info.systemIdentifier == nil {
		tracelog.ErrorLogger.Fatal("Our system lacks System Identifier, cannot proceed")
	}
	tracelog.ErrorLogger.FatalOnError(err)
	dial, err := net.Dial("tcp", destination)
	tracelog.ErrorLogger.FatalOnError(err)
	decoder := gob.NewDecoder(dial)
	var control PgControlData
	err = decoder.Decode(&control)
	tracelog.ErrorLogger.FatalOnError(err)
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
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.InfoLogger.Printf("Received file list of %v failes", len(fileList))
	_, lsnStr, _, err := runner.StartBackup("")
	tracelog.ErrorLogger.FatalOnError(err)
	lsn, err := ParseLSN(lsnStr)
	tracelog.ErrorLogger.FatalOnError(err)
	if lsn <= control.Checkpoint {
		tracelog.ErrorLogger.Fatal("Catchup destination is already ahead (our LSN %v, destination LSN %v).", lsn, control.Checkpoint)
	}

	encoder := gob.NewEncoder(dial)

	label, offsetMap, _, err := runner.StopBackup()
	tracelog.ErrorLogger.FatalOnError(err)

	sendFileCommand(dial, encoder, pgDataDirectory, fileList, control.Checkpoint)

	err = encoder.Encode(CatchupCommandDto{Contents: label, FileName: BackupLabelFilename, IsStr: true})
	tracelog.ErrorLogger.FatalOnError(err)
	err = encoder.Encode(CatchupCommandDto{Contents: offsetMap, FileName: TablespaceMapFilename, IsStr: true})
	tracelog.ErrorLogger.FatalOnError(err)
	ourPgControl, err := os.ReadFile(path.Join(pgDataDirectory, PgControlPath))
	tracelog.ErrorLogger.FatalOnError(err)
	err = encoder.Encode(CatchupCommandDto{BinaryContents: ourPgControl, FileName: PgControlPath})
	tracelog.ErrorLogger.FatalOnError(err)

	err = encoder.Encode(CatchupCommandDto{IsDone: true})
	tracelog.ErrorLogger.FatalOnError(err)
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

		return nil
	})
}

func HandleCatchupReceive(pgDataDirectory string, port int) {
	pgDataDirectory = utility.ResolveSymlink(pgDataDirectory)
	tracelog.InfoLogger.Printf("Receiving %v on port %v\n", pgDataDirectory, port)
	listen, err := net.Listen("tcp", fmt.Sprintf(":%v", port))
	tracelog.ErrorLogger.FatalOnError(err)
	conn, err := listen.Accept()
	sendControlAndFileList(pgDataDirectory, err, conn)
	decoder := gob.NewDecoder(conn)
	for {
		var cmd CatchupCommandDto
		err := decoder.Decode(&cmd)
		tracelog.ErrorLogger.FatalOnError(err)
		if cmd.IsDone {
			break
		}
		doRcvCommand(cmd, pgDataDirectory, conn)
	}
	tracelog.InfoLogger.Printf("Receive done")
}

func doRcvCommand(cmd CatchupCommandDto, directory string, conn net.Conn) {
	if cmd.BinaryContents != nil {
		tracelog.InfoLogger.Printf("Writing file %v", cmd.FileName)
		err := os.WriteFile(path.Join(directory, cmd.FileName), cmd.BinaryContents, 0666)
		tracelog.ErrorLogger.FatalOnError(err)
		return
	}
	if cmd.IsStr {
		tracelog.InfoLogger.Printf("Writing file %v", cmd.FileName)
		err := os.WriteFile(path.Join(directory, cmd.FileName), []byte(cmd.Contents), 0666)
		tracelog.ErrorLogger.FatalOnError(err)
		return
	}
}

type CatchupCommandDto struct {
	IsDone         bool
	IsIncremental  bool
	IsDelete       bool
	IsStr          bool
	FileSize       uint64
	FileName       string
	Contents       string
	BinaryContents []byte
}

func sendControlAndFileList(pgDataDirectory string, err error, conn net.Conn) {
	tracelog.ErrorLogger.FatalOnError(err)
	control, err := ExtractPgControl(pgDataDirectory)
	tracelog.InfoLogger.Printf("Our system id %v, need catchup from %v", control.SystemIdentifier, control.Checkpoint)
	tracelog.ErrorLogger.FatalOnError(err)
	encoder := gob.NewEncoder(conn)
	err = encoder.Encode(control)
	tracelog.ErrorLogger.FatalOnError(err)
	rcvFileList := receiveFileList(pgDataDirectory)
	err = encoder.Encode(rcvFileList)
	tracelog.ErrorLogger.FatalOnError(err)
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
