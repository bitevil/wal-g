package walg

import (
	"os"
	"log"
	"github.com/aws/aws-sdk-go/aws"
	"fmt"
	"path"
	"path/filepath"
	"runtime/pprof"
	"io/ioutil"
	"regexp"
	"time"
	"io"
	"strconv"
)

func HandleBackupList(pre *Prefix) {
	var bk = &Backup{
		Prefix: pre,
		Path:   aws.String(*pre.Server + "/basebackups_005/"),
	}
	backups, err := bk.GetBackups()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("name\tlast_modified\twal_segment_backup_start")
	for _, b := range backups {
		fmt.Printf("%v\t%v\t%v\n", b.Name, b.Time.Format(time.RFC3339), b.WalFileName)
	}
}

func HandleBackupFetch(backupName string, pre *Prefix, dirArc string, mem bool) (lsn *uint64) {
	lsn = DeltaFetchRecursion(backupName, pre, dirArc)

	if mem {
		f, err := os.Create("mem.prof")
		if err != nil {
			log.Fatal(err)
		}

		pprof.WriteHeapProfile(f)
		defer f.Close()
	}
	return
}

// This function composes Backup object and recursively searches for necessary base backup
func DeltaFetchRecursion(backupName string, pre *Prefix, dirArc string) (lsn *uint64) {
	var bk *Backup
	// Check if BACKUPNAME exists and if it does extract to DIRARC.
	if backupName != "LATEST" {
		bk = &Backup{
			Prefix: pre,
			Path:   aws.String(*pre.Server + "/basebackups_005/"),
			Name:   aws.String(backupName),
		}
		bk.Js = aws.String(*bk.Path + *bk.Name + "_backup_stop_sentinel.json")

		exists, err := bk.CheckExistence()
		if err != nil {
			log.Fatalf("%+v\n", err)
		}
		if !exists {
			log.Fatalf("Backup '%s' does not exist.\n", *bk.Name)
		}

		// Find the LATEST valid backup (checks against JSON file and grabs backup name) and extract to DIRARC.
	} else {
		bk = &Backup{
			Prefix: pre,
			Path:   aws.String(*pre.Server + "/basebackups_005/"),
		}

		latest, err := bk.GetLatest()
		if err != nil {
			log.Fatalf("%+v\n", err)
		}
		bk.Name = aws.String(latest)
	}
	var dto = fetchSentinel(*bk.Name, bk, pre)

	if dto.IsIncremental() {
		fmt.Printf("Delta from %v at LSN %x \n", *dto.IncrementFrom, *dto.IncrementFromLSN)
		DeltaFetchRecursion(*dto.IncrementFrom, pre, dirArc)
		fmt.Printf("%v fetched. Upgrading from LSN %x to LSN %x \n", *dto.IncrementFrom, *dto.IncrementFromLSN, dto.LSN)
	}

	UnwrapBackup(bk, dirArc, pre, dto)

	lsn = dto.LSN
	return
}

// Do the job of unpacking Backup object
func UnwrapBackup(bk *Backup, dirArc string, pre *Prefix, sentinel S3TarBallSentinelDto) {

	incrementBase := path.Join(dirArc, "increment_base")
	if !sentinel.IsIncremental() {
		var empty = true
		searchLambda := func(path string, info os.FileInfo, err error) error {
			if path != dirArc {
				empty = false
			}
			return nil
		}
		filepath.Walk(dirArc, searchLambda)

		if !empty {
			log.Fatalf("Directory %v for delta base must be empty", dirArc)
		}
	} else {
		defer func() {
			err := os.RemoveAll(incrementBase)
			if err != nil {
				log.Fatal(err)
			}
		}()

		err := os.MkdirAll(incrementBase, os.FileMode(0777))
		if err != nil {
			log.Fatal(err)
		}

		files, err := ioutil.ReadDir(dirArc)
		if err != nil {
			log.Fatal(err)
		}

		for _, f := range files {
			objName := f.Name()
			if objName != "increment_base" {
				err := os.Rename(path.Join(dirArc, objName), path.Join(incrementBase, objName))
				if err != nil {
					log.Fatal(err)
				}
			}
		}

		for fileName, fd := range sentinel.Files {
			if !fd.IsSkipped {
				continue
			}
			fmt.Printf("Skipped file %v\n", fileName)
			targetPath := path.Join(dirArc, fileName)
			// this path is only used for increment restoration
			incrementalPath := path.Join(incrementBase, fileName)
			err = MoveFileAndCreateDirs(incrementalPath, targetPath, fileName)
			if err != nil {
				log.Fatal(err, "Failed to move skipped file for "+targetPath+" "+fileName)
			}
		}

	}

	var allKeys []string
	var keys []string
	allKeys, err := bk.GetKeys()
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
	keys = allKeys[:len(allKeys)-1]
	f := &FileTarInterpreter{
		NewDir:             dirArc,
		Sentinel:           sentinel,
		IncrementalBaseDir: incrementBase,
	}
	out := make([]ReaderMaker, len(keys))
	for i, key := range keys {
		s := &S3ReaderMaker{
			Backup:     bk,
			Key:        aws.String(key),
			FileFormat: CheckType(key),
		}
		out[i] = s
	}
	// Extract all compressed tar members except `pg_control.tar.lz4` if WALG version backup.
	err = ExtractAll(f, out)
	if serr, ok := err.(*UnsupportedFileTypeError); ok {
		log.Fatalf("%v\n", serr)
	} else if err != nil {
		log.Fatalf("%+v\n", err)
	}
	// Check name for backwards compatibility. Will check for `pg_control` if WALG version of backup.
	re := regexp.MustCompile(`^([^_]+._{1}[^_]+._{1})`)
	match := re.FindString(*bk.Name)
	if match == "" {
		// Extract pg_control last. If pg_control does not exist, program exits with error code 1.
		name := *bk.Path + *bk.Name + "/tar_partitions/pg_control.tar.lz4"
		pgControl := &Archive{
			Prefix:  pre,
			Archive: aws.String(name),
		}

		exists, err := pgControl.CheckExistence()
		if err != nil {
			log.Fatalf("%+v\n", err)
		}

		if exists {
			sentinel := make([]ReaderMaker, 1)
			sentinel[0] = &S3ReaderMaker{
				Backup:     bk,
				Key:        aws.String(name),
				FileFormat: CheckType(name),
			}
			err := ExtractAll(f, sentinel)
			if serr, ok := err.(*UnsupportedFileTypeError); ok {
				log.Fatalf("%v\n", serr)
			} else if err != nil {
				log.Fatalf("%+v\n", err)
			}
			fmt.Printf("\nBackup extraction complete.\n")
		} else {
			log.Fatal("Corrupt backup: missing pg_control")
		}
	}
}

func GetDeltaConfig() (max_deltas int, from_full bool) {
	stepsStr, hasSteps := os.LookupEnv("WALG_DELTA_MAX_STEPS")
	var err error
	if hasSteps {
		max_deltas, err = strconv.Atoi(stepsStr)
		if err != nil {
			log.Fatal("Unable to parse WALG_DELTA_MAX_STEPS ", err)
		}
	}
	origin, hasOrigin := os.LookupEnv("WALG_DELTA_ORIGIN")
	if hasOrigin {
		switch origin {
		case "LATEST":
			break;
		case "LATEST_FULL":
			from_full = false
			break;
		default:
			log.Fatal("Unknown WALG_DELTA_ORIGIN:", origin)
		}
	}
	return
}

func HandleBackupPush(dirArc string, tu *TarUploader, pre *Prefix) {
	max_deltas, from_full := GetDeltaConfig()

	var bk = &Backup{
		Prefix: pre,
		Path:   aws.String(*pre.Server + "/basebackups_005/"),
	}

	var dto S3TarBallSentinelDto
	var latest string
	var err error
	incrementCount := 1

	if max_deltas > 0 {
		latest, err = bk.GetLatest()
		if err != LatestNotFound {
			if err != nil {
				log.Fatalf("%+v\n", err)
			}
			dto = fetchSentinel(latest, bk, pre)
			if dto.IncrementCount != nil {
				incrementCount = *dto.IncrementCount + 1
			}

			if incrementCount > max_deltas {
				fmt.Println("Reached max delta steps. Doing full backup.")
				dto = S3TarBallSentinelDto{}
			} else if dto.LSN == nil {
				fmt.Println("LATEST backup was made without increment feature. Fallback to full backup with increment LSN marker.")
			} else {
				if from_full {
					fmt.Println("Delta will be made from full backup.")
					latest = *dto.IncrementFullName
					dto = fetchSentinel(latest, bk, pre)
				}
				fmt.Printf("Incremental backup from %v with LSN %x. \n", latest, *dto.LSN)
			}
		}
	}

	bundle := &Bundle{
		MinSize:            int64(1000000000), //MINSIZE = 1GB
		IncrementFromLsn:   dto.LSN,
		IncrementFromFiles: dto.Files,
	}
	if dto.Files == nil {
		bundle.IncrementFromFiles = make(map[string]BackupFileDescription)
	}

	// Connect to postgres and start/finish a nonexclusive backup.
	conn, err := Connect()
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
	name, lsn, err := bundle.StartBackup(conn, time.Now().String())
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
	// Start a new tar bundle and walk the DIRARC directory and upload to S3.
	bundle.Tbm = &S3TarBallMaker{
		BaseDir:          filepath.Base(dirArc),
		Trim:             dirArc,
		BkupName:         name,
		Tu:               tu,
		Lsn:              &lsn,
		IncrementFromLsn: dto.LSN,
		IncrementFrom:    latest,
	}
	bundle.NewTarBall()
	fmt.Println("Walking ...")
	err = filepath.Walk(dirArc, bundle.TarWalker)
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
	err = bundle.Tb.CloseTar()
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
	// Upload `pg_control`.
	err = bundle.HandleSentinel()
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
	// Stops backup and write/upload postgres `backup_label` and `tablespace_map` files
	err = bundle.HandleLabelFiles(conn)
	if err != nil {
		log.Fatalf("%+v\n", err)
	}

	timelineChanged := bundle.CheckTimelineChanged(conn)
	var sentinel *S3TarBallSentinelDto

	if !timelineChanged {
		sentinel = &S3TarBallSentinelDto{
			LSN:              &lsn,
			IncrementFromLSN: dto.LSN,
		}
		if dto.LSN != nil {
			sentinel.IncrementFrom = &latest
			sentinel.IncrementFullName = &latest
			if dto.IsIncremental() {
				sentinel.IncrementFullName = dto.IncrementFullName
			}
			sentinel.IncrementCount = &incrementCount
		}

		dto.Files = bundle.Tb.GetFiles()
	}

	// Wait for all uploads to finish.
	err = bundle.Tb.Finish(sentinel)
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
}

func HandleWALFetch(pre *Prefix, dirArc string, backupName string) {
	a := &Archive{
		Prefix:  pre,
		Archive: aws.String(*pre.Server + "/wal_005/" + dirArc + ".lzo"),
	}
	// Check existence of compressed LZO WAL file
	exists, err := a.CheckExistence()
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
	var crypter = OpenPGPCrypter{}
	if exists {
		arch, err := a.GetArchive()
		if err != nil {
			log.Fatalf("%+v\n", err)
		}

		if crypter.IsUsed() {
			var reader io.Reader
			reader, err = crypter.Decrypt(arch)
			if err != nil {
				log.Fatalf("%v\n", err)
			}
			arch = ReadCascadeClose{reader, arch}
		}

		f, err := os.Create(backupName)
		if err != nil {
			log.Fatalf("%v\n", err)
		}

		err = DecompressLzo(f, arch)
		if err != nil {
			log.Fatalf("%+v\n", err)
		}
		f.Close()
	} else if !exists {
		// Check existence of compressed LZ4 WAL file
		a.Archive = aws.String(*pre.Server + "/wal_005/" + dirArc + ".lz4")
		exists, err = a.CheckExistence()
		if err != nil {
			log.Fatalf("%+v\n", err)
		}

		if exists {
			arch, err := a.GetArchive()
			if err != nil {
				log.Fatalf("%+v\n", err)
			}

			if crypter.IsUsed() {
				var reader io.Reader
				reader, err = crypter.Decrypt(arch)
				if err != nil {
					log.Fatalf("%v\n", err)
				}
				arch = ReadCascadeClose{reader, arch}
			}

			f, err := os.Create(backupName)
			if err != nil {
				log.Fatalf("%v\n", err)
			}

			err = DecompressLz4(f, arch)
			if err != nil {
				log.Fatalf("%+v\n", err)
			}
			f.Close()
		} else {
			log.Fatalf("Archive '%s' does not exist.\n", dirArc)
		}
	}
}

func HandleWALPush(tu *TarUploader, dirArc string) {
	path, err := tu.UploadWal(dirArc)
	if re, ok := err.(Lz4Error); ok {
		log.Fatalf("FATAL: could not upload '%s' due to compression error.\n%+v\n", path, re)
	} else if err != nil {
		log.Printf("upload: could not upload '%s' after %v retries\n", path, tu.MaxRetries)
		log.Fatalf("FATAL%+v\n", err)
	}
}