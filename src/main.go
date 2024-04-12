package main

import (
	"fmt"
	"github.com/tsuna/gohbase/hrpc"
	"news-crawler/src/modules/crawler"
	"news-crawler/src/modules/saver"
)

const (
	ZkConnStr = "tools-linux:2181"
)

func saveNews() {
	hdfsSaver := saver.Saver{ZkQuorum: ZkConnStr}
	craw := crawler.Crawler{}
	// 爬取新闻
	src := crawler.SrcXinhuanet
	newsList, err := craw.PullNews(src)
	if err != nil {
		panic(err)
	}
	// 保存新闻
	err = hdfsSaver.SaveNews(newsList)
	if err != nil {
		panic(err)
	}
}

func scanNews() {
	hdfsSaver := saver.Saver{ZkQuorum: ZkConnStr}
	err := hdfsSaver.ScanNews(func(result *hrpc.Result) (err error) {
		for _, cell := range result.Cells {
			//cell.Qualifier
			//cell.Value
			//cell.Row
			fmt.Print(cell.String())
		}
		fmt.Println()
		return
	})
	if err != nil {
		panic(err)
	}
}

func deleteNews() {
	hdfsSaver := saver.Saver{ZkQuorum: ZkConnStr}
	res, err := hdfsSaver.DeleteNews("1")
	if err != nil {
		panic(err)
	}
	fmt.Println(res)
	res, err = hdfsSaver.DeleteNews("2")
	if err != nil {
		panic(err)
	}
	fmt.Println(res)
}

func main() {
	saveNews()
	scanNews()
	deleteNews()
}
