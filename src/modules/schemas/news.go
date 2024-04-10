package schemas

const (
	TabNews = "news"
)

type News struct {
	Title   string `json:"title"`
	Content string `json:"content"`
	Src     string `json:"src"`
	Date    string `json:"date"`
}

func (n *News) ToHBaseValues() map[string]map[string][]byte {
	return map[string]map[string][]byte{"default": {
		"title":   []byte(n.Title),
		"content": []byte(n.Content),
		"src":     []byte(n.Src),
		"date":    []byte(n.Date),
	}}
}
