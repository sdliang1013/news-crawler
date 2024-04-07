package hdfs

import "github.com/colinmarc/hdfs/v2"

func NewClient(address string) (*hdfs.Client, error) {
	return hdfs.NewClient(hdfs.ClientOptions{
		Addresses: []string{address},
		User:      "root",
	})
}