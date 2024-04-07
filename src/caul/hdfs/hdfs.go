package hdfs

import (
	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/parquet"
	"github.com/apache/arrow/go/v15/parquet/pqarrow"
	"github.com/colinmarc/hdfs/v2"
)

func NewClient(address string, user string) (*hdfs.Client, error) {
	return hdfs.NewClient(hdfs.ClientOptions{
		Addresses: []string{address},
		User:      user,
	})
}

func SaveFile(file *hdfs.FileWriter, schema *arrow.Schema, records []arrow.Record) (err error) {
	var writer *pqarrow.FileWriter
	writer, err = pqarrow.NewFileWriter(schema, file,
		parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	if err != nil {
		return
	}
	defer func(writer *pqarrow.FileWriter) {
		_ = writer.Close()
	}(writer)
	for _, record := range records {
		err = writer.Write(record)
		if err != nil {
			return
		}
		record.Release()
	}
	return
}

func FileWriter(client *hdfs.Client, filePath string) (writer *hdfs.FileWriter, err error) {
	_, err = client.Stat(filePath)
	if err != nil { // not exists
		writer, err = client.Create(filePath)
	} else { // exists
		writer, err = client.Append(filePath)
	}
	return
}
