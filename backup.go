package walg

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/pkg/errors"
	"io"
	"sort"
	"strings"
)

// WalFiles represent any file generated by WAL-G.
type WalFiles interface {
	CheckExistence() (bool, error)
}

// ReaderMaker is the generic interface used by extract. It
// allows for ease of handling different file formats.
type ReaderMaker interface {
	Reader() (io.ReadCloser, error)
	Format() string
	Path() string
}

// S3ReaderMaker handles cases where backups need to be uploaded to
// S3.
type S3ReaderMaker struct {
	Backup     *Backup
	Key        *string
	FileFormat string
}

func (s *S3ReaderMaker) Format() string { return s.FileFormat }
func (s *S3ReaderMaker) Path() string   { return *s.Key }

// Reader creates a new S3 reader for each S3 object.
func (s *S3ReaderMaker) Reader() (io.ReadCloser, error) {
	input := &s3.GetObjectInput{
		Bucket: s.Backup.Prefix.Bucket,
		Key:    s.Key,
	}

	rdr, err := s.Backup.Prefix.Svc.GetObject(input)
	if err != nil {
		return nil, errors.Wrap(err, "S3 Reader: s3.GetObject failed")
	}
	return rdr.Body, nil

}

// Prefix contains the S3 service client, bucket and string.
type Prefix struct {
	Svc    s3iface.S3API
	Bucket *string
	Server *string
}

// Backup contains information about a valid backup
// generated and uploaded by WAL-G.
type Backup struct {
	Prefix *Prefix
	Path   *string
	Name   *string
	Js     *string
}

var LatestNotFound = errors.New("LATEST backup not found")

// GetLatest sorts the backups by last modified time
// and returns the latest backup key.
func (b *Backup) GetLatest() (string, error) {
	objects := &s3.ListObjectsInput{
		Bucket:    b.Prefix.Bucket,
		Prefix:    b.Path,
		Delimiter: aws.String("/"),
	}

	backups, err := b.Prefix.Svc.ListObjects(objects)
	if err != nil {
		return "", errors.Wrap(err, "GetLatest: s3.ListObjectsV2 failed")

	}

	count := len(backups.Contents)

	if count==0 {
		return "", LatestNotFound
	}

	sortTimes := make([]BackupTime, count)

	for i, ob := range backups.Contents {
		key := *ob.Key
		time := *ob.LastModified
		sortTimes[i] = BackupTime{stripNameBackup(key), time}
	}

	sort.Sort(TimeSlice(sortTimes))

	return sortTimes[0].Name, nil
}

// Strips the backup key and returns it in its base form `base_...`.
func stripNameBackup(key string) string {
	all := strings.SplitAfter(key, "/")
	name := strings.Split(all[len(all)-1], "_backup")[0]
	return name
}

// CheckExistence checks that the specified backup exists.
func (b *Backup) CheckExistence() (bool, error) {
	js := &s3.HeadObjectInput{
		Bucket: b.Prefix.Bucket,
		Key:    b.Js,
	}

	_, err := b.Prefix.Svc.HeadObject(js)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			switch awsErr.Code() {
			case "NotFound":
				return false, nil
			default:
				return false, awsErr
			}

		}
	}
	return true, nil
}

// GetKeys returns all the keys for the files in the specified backup.
func (b *Backup) GetKeys() ([]string, error) {
	objects := &s3.ListObjectsInput{
		Bucket: b.Prefix.Bucket,
		Prefix: aws.String(*b.Path + *b.Name + "/tar_partitions"),
	}

	files, err := b.Prefix.Svc.ListObjects(objects)
	if err != nil {
		return nil, errors.Wrap(err, "GetKeys: s3.ListObjectsV2 failed")
	}

	arr := make([]string, len(files.Contents))

	for i, ob := range files.Contents {
		key := *ob.Key
		arr[i] = key
	}

	return arr, nil
}

// Archive contains information associated with
// a WAL archive.
type Archive struct {
	Prefix  *Prefix
	Archive *string
}

// CheckExistence checks that the specified WAL file exists.
func (a *Archive) CheckExistence() (bool, error) {
	arch := &s3.HeadObjectInput{
		Bucket: a.Prefix.Bucket,
		Key:    a.Archive,
	}

	_, err := a.Prefix.Svc.HeadObject(arch)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			switch awsErr.Code() {
			case "NotFound":
				return false, nil
			default:
				return false, awsErr
			}
		}
	}
	return true, nil
}

// GetArchive downloads the specified archive from S3.
func (a *Archive) GetArchive() (io.ReadCloser, error) {
	input := &s3.GetObjectInput{
		Bucket: a.Prefix.Bucket,
		Key:    a.Archive,
	}

	archive, err := a.Prefix.Svc.GetObject(input)
	if err != nil {
		return nil, errors.Wrap(err, "GetArchive: s3.GetObject failed")
	}

	return archive.Body, nil
}
