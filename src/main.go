package main

import (
	"news-crawler/src/caul/hdfs"
	"news-crawler/src/modules/crawler"
	"news-crawler/src/modules/saver"
	"news-crawler/src/modules/schemas"
)

var ()

func main() {
	client, err := hdfs.NewClient("db-linux:8020", "root")
	if err != nil {
		panic(err)
	}
	hdfsSaver := saver.Saver{Client: client}
	craw := crawler.Crawler{}
	// 爬取新闻
	var newsList []schemas.News
	newsList, err = craw.PullNews()
	if err != nil {
		panic(err)
	}
	// 保存新闻
	//year, month, day := time.Now().Date()
	err = hdfsSaver.SaveToHdfs("/materials/news/test.par", newsList)
	if err != nil {
		panic(err)
	}
}
