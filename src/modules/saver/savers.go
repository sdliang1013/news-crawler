package saver

import (
	"fmt"
	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/apache/arrow/go/v15/parquet"
	"github.com/apache/arrow/go/v15/parquet/pqarrow"
	"github.com/colinmarc/hdfs/v2"
	"news-crawler/src/modules/schemas"
)

type Saver struct {
	Client *hdfs.Client
}

func (s *Saver) Close() error {
	return s.Client.Close()
}

func (s *Saver) SaveNews(filePath string, newsList []schemas.News) (err error) {
	// 将新闻数据转换为 Parquet 格式
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "title", Type: arrow.BinaryTypes.String},
			{Name: "content", Type: arrow.BinaryTypes.String},
			{Name: "src", Type: arrow.BinaryTypes.String},
			{Name: "date", Type: arrow.PrimitiveTypes.Date32},
		},
		nil,
	)

	// 填充列数据
	var records []arrow.Record
	records, err = s.toRecords(schema, newsList)
	if err != nil {
		return
	}

	// 将表数据转换为 Parquet 格式
	err = s.saveToHdfs(filePath, schema, records)
	if err != nil {
		return
	}

	fmt.Println("新闻信息已保存到 HDFS 中")
	return
}

func (s *Saver) toRecords(schema *arrow.Schema, newsList []schemas.News) (records []arrow.Record, err error) {
	recordBuilder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer recordBuilder.Release()

	for _, news := range newsList {
		//title
		err = recordBuilder.Field(0).AppendValueFromString(news.Title)
		if err != nil {
			return
		}
		//content
		err = recordBuilder.Field(1).AppendValueFromString(news.Content)
		if err != nil {
			return
		}
		//src
		err = recordBuilder.Field(2).AppendValueFromString(news.Src)
		if err != nil {
			return
		}
		//date
		err = recordBuilder.Field(3).AppendValueFromString(news.Date)
		if err != nil {
			return
		}
		records = append(records, recordBuilder.NewRecord())
	}
	return
}

func (s *Saver) saveToHdfs(filePath string, schema *arrow.Schema, records []arrow.Record) (err error) {
	var file *hdfs.FileWriter
	var writer *pqarrow.FileWriter

	file, err = s.Client.Create(filePath)
	if err != nil {
		return
	}
	props := parquet.NewWriterProperties()
	writer, err = pqarrow.NewFileWriter(schema, file, props, pqarrow.DefaultWriterProps())
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
