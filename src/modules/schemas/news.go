package schemas

import (
	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
)

type News struct {
	Title   string `json:"title"`
	Content string `json:"content"`
	Src     string `json:"src"`
	Date    string `json:"date"`
}

func (n *News) ToArrowRecord(rb *array.RecordBuilder) (record arrow.Record, err error) {
	//title
	err = rb.Field(0).AppendValueFromString(n.Title)
	if err != nil {
		return
	}
	//content
	err = rb.Field(1).AppendValueFromString(n.Content)
	if err != nil {
		return
	}
	//src
	err = rb.Field(2).AppendValueFromString(n.Src)
	if err != nil {
		return
	}
	//date
	err = rb.Field(3).AppendValueFromString(n.Date)
	if err != nil {
		return
	}
	record = rb.NewRecord()
	return
}

func NewsToArrowSchema(metadata *arrow.Metadata) *arrow.Schema {
	return arrow.NewSchema(
		[]arrow.Field{
			{Name: "title", Type: arrow.BinaryTypes.String},
			{Name: "content", Type: arrow.BinaryTypes.String},
			{Name: "src", Type: arrow.BinaryTypes.String},
			{Name: "date", Type: arrow.PrimitiveTypes.Date32},
		},
		metadata,
	)
}
