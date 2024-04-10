package saver

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/hrpc"
	"news-crawler/src/modules/schemas"
)

type Saver struct {
	Client gohbase.Client
}

func (s *Saver) Close() {
	s.Client.Close()
}

func (s *Saver) SaveToHBase(newList []schemas.News) (err error) {
	var records []*hrpc.Mutate
	// 将新闻数据转换为 Parquet 格式
	records, err = s.toRecords(newList)
	if err != nil {
		return
	}
	// 保存数据
	for _, record := range records {
		_, err = s.Client.Put(record)
		if err != nil {
			return err
		}
	}
	fmt.Println("新闻信息已保存到 HDFS 中")
	return
}

func (s *Saver) toRecords(newsList []schemas.News) (records []*hrpc.Mutate, err error) {
	var record *hrpc.Mutate
	var random uuid.UUID
	for _, news := range newsList {
		random, err = uuid.NewRandom()
		if err != nil {
			return
		}
		record, err = hrpc.NewPutStr(context.Background(), schemas.TabNews, random.String(), news.ToHBaseValues())
		if err != nil {
			return
		}
		records = append(records, record)
	}
	return
}
