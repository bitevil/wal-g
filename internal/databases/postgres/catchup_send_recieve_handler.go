package postgres

import (
	"encoding/gob"
	"fmt"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
	"net"
)

func HandleCatchupSend(pgDataDirectory string, destination string) {
	pgDataDirectory = utility.ResolveSymlink(pgDataDirectory)
	tracelog.InfoLogger.Printf("Sending %v to %v\n", pgDataDirectory, destination)
	info, err := GetPgServerInfo()
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
}

func HandleCatchupReceive(pgDataDirectory string, port int) {
	pgDataDirectory = utility.ResolveSymlink(pgDataDirectory)
	tracelog.InfoLogger.Printf("Receiving %v on port %v\n", pgDataDirectory, port)
	listen, err := net.Listen("tcp", fmt.Sprintf(":%v", port))
	tracelog.ErrorLogger.FatalOnError(err)
	conn, err := listen.Accept()
	sendControlFile(pgDataDirectory, err, conn)
	//conn.Read(make([]byte, 10))
}

func sendControlFile(pgDataDirectory string, err error, conn net.Conn) {
	tracelog.ErrorLogger.FatalOnError(err)
	control, err := ExtractPgControl(pgDataDirectory)
	tracelog.InfoLogger.Printf("Our system id %v, need catchup from %v", control.SystemIdentifier, control.Checkpoint)
	tracelog.ErrorLogger.FatalOnError(err)
	encoder := gob.NewEncoder(conn)
	err = encoder.Encode(control)
}
