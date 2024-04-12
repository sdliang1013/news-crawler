package schemas

const (
	TabNews = "news"
)

type News struct {
	Id      string
	Title   string `json:"title"`
	Content string `json:"content"`
	Src     string `json:"src"`
	Date    string `json:"date"`
}

func NewsFamily() map[string]map[string]string {
	return map[string]map[string]string{"default": {
		"title":   "title",
		"content": "content",
		"src":     "src",
		"date":    "date",
	}}
}

func (n *News) ToHBaseValues() map[string]map[string][]byte {
	return map[string]map[string][]byte{"default": {
		"title":   []byte(n.Title),
		"content": []byte(n.Content),
		"src":     []byte(n.Src),
		"date":    []byte(n.Date),
	}}
}
