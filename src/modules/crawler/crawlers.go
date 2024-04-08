package crawler

import "news-crawler/src/modules/schemas"

const (
	SrcGoogle    = "google"
	SrcBing      = "bing"
	SrcYahoo     = "yahoo"     //雅虎
	SrcSina      = "sina"      //新浪
	Src163       = "163"       //网易
	SrcQQ        = "qq"        //腾讯
	SrcIfeng     = "ifeng"     //凤凰
	SrcToutiao   = "toutiao"   //头条
	SrcXinhuanet = "xinhuanet" //新华网
)

type Crawler struct {
}

func (c *Crawler) PullNews(src string) (newsList []schemas.News, err error) {
	srcDesc := getSrcDesc(src)
	// todo 按来源爬取新闻
	newsList = []schemas.News{
		{
			Title:   "中国共产党第十九次全国代表大会胜利闭幕",
			Content: "中国共产党第十九次全国代表大会于2017年10月18日至24日在北京召开。大会以习近平新时代中国特色社会主义思想为指导，全面贯彻党的十八大、十八届三中全会、十八届四中全会、十八届五中全会精神，审议并通过了《中国共产党第十九次全国代表大会关于修改中国共产党章程的决议》，选举产生了以习近平同志为核心的新一届中央领导集体。",
			Src:     srcDesc,
			Date:    "2017-10-24",
		},
		{
			Title:   "习近平总书记在十九大闭幕式上的讲话",
			Content: "同志们，朋友们，女士们、先生们，中国共产党第十九次全国代表大会，在全党、全国人民的热切期盼中，胜利闭幕了！",
			Src:     srcDesc,
			Date:    "2017-10-24",
		},
	}
	return
}

func getSrcDesc(src string) string {
	switch src {
	case SrcGoogle:
		return "谷歌新闻"
	case SrcBing:
		return "必应新闻"
	case SrcYahoo:
		return "雅虎新闻"
	case SrcSina:
		return "新浪新闻"
	case Src163:
		return "网易新闻"
	case SrcQQ:
		return "腾讯新闻"
	case SrcIfeng:
		return "凤凰网"
	case SrcToutiao:
		return "今日头条"
	case SrcXinhuanet:
		return "新华网"
	default:
		return "未知来源"
	}
}
