package walparser

import (
	"bytes"
	"github.com/pkg/errors"
	"github.com/x4m/wal-g/walparser/parsingutil"
	"io"
)

var ZeroPageHeaderError = errors.New("page header contains only zeroes, maybe it is a part .partial file or this page follow WAL-switch record")
var InvalidPageHeaderError = errors.New("invalid page header")

func tryReadXLogRecordData(alignedReader *AlignedReader) (data []byte, whole bool, err error) {
	err = alignedReader.ReadToAlignment()
	if err != nil {
		if err == io.EOF {
			return nil, false, nil
		}
		return nil, false, err
	}
	headerData := make([]byte, XLogRecordHeaderSize)
	readCount, err := alignedReader.Read(headerData)
	if err != nil && err != io.EOF {
		return nil, false, err
	}
	if readCount < XLogRecordHeaderSize {
		if readCount > 0 && allZero(headerData[:readCount]) { // end of last non zero page of .partial file
			return nil, false, ZeroRecordHeaderError
		}
		return headerData[:readCount], false, nil // header don't fit into the page
	}
	recordHeader, err := readXLogRecordHeader(bytes.NewReader(headerData)) // zero header error is ok for partial page here
	if err != nil {
		return nil, false, err
	}
	recordContent := make([]byte, minUint32(recordHeader.TotalRecordLength-XLogRecordHeaderSize, uint32(WalPageSize)))
	readCount, err = alignedReader.Read(recordContent)
	if err != nil && err != io.EOF {
		return nil, false, err
	}
	wholeRecord := uint32(readCount) == recordHeader.TotalRecordLength-XLogRecordHeaderSize
	return concatByteSlices(headerData, recordContent[:readCount]), wholeRecord, nil
}

func readXLogLongPageHeaderData(reader io.Reader) error {
	var systemID uint64
	var segmentSize uint32
	var xLogBlockSize uint32
	return parsingutil.ParseMultipleFieldsFromReader([]parsingutil.FieldToParse{
		{Field: &systemID, Name: "systemID"},
		{Field: &segmentSize, Name: "segmentSize"},
		{Field: &xLogBlockSize, Name: "xLogBlockSize"},
	}, reader)
}

// If header is long, then long header data is read from reader and thrown away
func readXLogPageHeader(reader io.Reader) (*XLogPageHeader, error) {
	pageHeader := XLogPageHeader{}
	err := parsingutil.ParseMultipleFieldsFromReader([]parsingutil.FieldToParse{
		{Field: &pageHeader.Magic, Name: "magic"},
		{Field: &pageHeader.Info, Name: "info"},
		{Field: &pageHeader.TimeLineID, Name: "timeLineID"},
		{Field: &pageHeader.PageAddress, Name: "pageAddress"},
		{Field: &pageHeader.RemainingDataLen, Name: "remainingDataLen"},
	}, reader)
	if err != nil {
		return nil, err
	}
	if pageHeader.isZero() {
		return nil, ZeroPageHeaderError
	}

	if !pageHeader.IsValid() {
		return nil, InvalidPageHeaderError
	}

	// read long header data from reader
	if pageHeader.IsLong() {
		err = readXLogLongPageHeaderData(reader)
		if err != nil {
			return nil, err
		}
	}

	return &pageHeader, nil
}
