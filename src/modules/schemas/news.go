package schemas

type News struct {
	Title   string `json:"title"`
	Content string `json:"content"`
	Src     string `json:"src"`
	Date    string `json:"date"`
}
