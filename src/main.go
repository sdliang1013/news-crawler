package main

import (
	"github.com/tsuna/gohbase"
	"news-crawler/src/modules/crawler"
	"news-crawler/src/modules/saver"
)

func main() {
	client := gohbase.NewClient("tools-linux:2181")
	hdfsSaver := saver.Saver{Client: client}
	craw := crawler.Crawler{}
	// 爬取新闻
	src := crawler.SrcXinhuanet
	newsList, err := craw.PullNews(src)
	if err != nil {
		panic(err)
	}
	// 保存新闻
	err = hdfsSaver.SaveToHBase(newsList)
	if err != nil {
		panic(err)
	}
}
