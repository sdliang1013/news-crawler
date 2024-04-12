package saver

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/hrpc"
	"io"
	"news-crawler/src/modules/schemas"
)

type Saver struct {
	ZkQuorum string
	Opts     []gohbase.Option
}

func (s *Saver) SaveNews(newList []schemas.News) (err error) {
	var records []hrpc.Call
	// 将新闻数据转换为 Parquet 格式
	records, err = s.toRecords(newList)
	if err != nil {
		return
	}
	// 创建表
	err = s.createTable()
	if err != nil {
		return
	}
	// 保存数据
	client := s.newClient()
	defer client.Close()
	res, ok := client.SendBatch(context.Background(), records)
	if !ok {
		if len(res) > 0 {
			err = res[0].Error
		}
		return err
	}
	fmt.Println("新闻信息已保存到 HBase 中")
	return
}

func (s *Saver) DeleteNews(rowId string) (result *hrpc.Result, err error) {
	// new client
	client := s.newClient()
	defer client.Close()
	// delete rowId
	var delStr *hrpc.Mutate
	delStr, err = hrpc.NewDelStr(context.Background(), schemas.TabNews, rowId, nil)
	if err != nil {
		return
	}
	return client.Delete(delStr)
}

func (s *Saver) ScanNews(handler func(result *hrpc.Result) error) (err error) {
	// new client
	client := s.newClient()
	defer client.Close()
	// get cursor
	var scan *hrpc.Scan
	scan, err = hrpc.NewScanStr(context.Background(), schemas.TabNews)
	if err != nil {
		return err
	}
	cursor := client.Scan(scan)
	defer func(scanner hrpc.Scanner) {
		_ = scanner.Close()
	}(cursor)
	// scan result
	var next *hrpc.Result
	for {
		next, err = cursor.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return
		}
		err = handler(next)
		if err != nil {
			return
		}
	}
}

func (s *Saver) newClient() gohbase.Client {
	return gohbase.NewClient(s.ZkQuorum, s.Opts...)
}
func (s *Saver) newAdminClient() gohbase.AdminClient {
	return gohbase.NewAdminClient(s.ZkQuorum, s.Opts...)
}

func (s *Saver) createTable() (err error) {
	// check table exists
	var tab *hrpc.Get
	tab, err = hrpc.NewGetStr(context.Background(), schemas.TabNews, "whatever")
	if err != nil {
		return
	}
	client := s.newClient()
	defer client.Close()
	_, err = client.Get(tab)
	// create table if not exists
	if err == gohbase.TableNotFound {
		return s.newAdminClient().CreateTable(hrpc.NewCreateTable(context.Background(),
			[]byte(schemas.TabNews), schemas.NewsFamily()))
	}
	return
}

func (s *Saver) toRecords(newsList []schemas.News) (records []hrpc.Call, err error) {
	var record hrpc.Call
	var random uuid.UUID
	for _, news := range newsList {
		// check ID
		if news.Id == "" {
			random, err = uuid.NewRandom()
			if err != nil {
				return
			}
			news.Id = random.String()
		}
		record, err = hrpc.NewPutStr(context.Background(), schemas.TabNews, news.Id, news.ToHBaseValues())
		if err != nil {
			return
		}
		records = append(records, record)
	}
	return
}
