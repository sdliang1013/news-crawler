package crawler

import "news-crawler/src/modules/schemas"

type Crawler struct {
}

func (c *Crawler) PullNews() (newsList []schemas.News, err error) {
	newsList = []schemas.News{
		{
			Title:   "中国共产党第十九次全国代表大会胜利闭幕",
			Content: "中国共产党第十九次全国代表大会于2017年10月18日至24日在北京召开。大会以习近平新时代中国特色社会主义思想为指导，全面贯彻党的十八大、十八届三中全会、十八届四中全会、十八届五中全会精神，审议并通过了《中国共产党第十九次全国代表大会关于修改中国共产党章程的决议》，选举产生了以习近平同志为核心的新一届中央领导集体。",
			Src:     "新华网",
			Date:    "2017-10-24",
		},
		{
			Title:   "习近平总书记在十九大闭幕式上的讲话",
			Content: "同志们，朋友们，女士们、先生们，中国共产党第十九次全国代表大会，在全党、全国人民的热切期盼中，胜利闭幕了！",
			Src:     "新华网",
			Date:    "2017-10-24",
		},
	}
	return
}
