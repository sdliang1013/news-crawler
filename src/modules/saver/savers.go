package saver

import (
	"fmt"
	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/colinmarc/hdfs/v2"
	cHdfs "news-crawler/src/caul/hdfs"
	"news-crawler/src/modules/schemas"
)

type Saver struct {
	Client *hdfs.Client
}

func (s *Saver) Close() error {
	return s.Client.Close()
}

func (s *Saver) SaveToHdfs(filePath string, newsList []schemas.News) (err error) {
	var records []arrow.Record
	// 将新闻数据转换为 Parquet 格式
	schema := schemas.NewsToArrowSchema(nil)
	records, err = s.toRecords(schema, newsList)
	if err != nil {
		return
	}
	// 保存数据
	var file *hdfs.FileWriter
	file, err = cHdfs.FileWriter(s.Client, filePath)
	if err != nil {
		return
	}
	defer func(file *hdfs.FileWriter) {
		_ = file.Close()
	}(file)
	err = cHdfs.SaveFile(file, schema, records)
	if err != nil {
		return
	}
	fmt.Println("新闻信息已保存到 HDFS 中")
	return
}

func (s *Saver) toRecords(schema *arrow.Schema, newsList []schemas.News) (records []arrow.Record, err error) {
	recordBuilder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer recordBuilder.Release()
	var record arrow.Record
	for _, news := range newsList {
		record, err = news.ToArrowRecord(recordBuilder)
		if err != nil {
			return
		}
		records = append(records, record)
	}
	return
}
