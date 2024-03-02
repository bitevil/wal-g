package postgres

import "github.com/wal-g/tracelog"

func HandleCatchupSend(pgDataDirectory string, destination string) {
	tracelog.InfoLogger.Printf("Sending %v to %v\n", pgDataDirectory, destination)
}

func HandleCatchupReceive(pgDataDirectory string, port int) {
	tracelog.InfoLogger.Printf("Receiving %v on port %v\n", pgDataDirectory, port)
}
