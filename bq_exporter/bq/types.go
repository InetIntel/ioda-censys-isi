package bqutil

import (
// "cloud.google.com/go/bigquery"
)

type IpInfo struct {
	Ip     string `bigquery:"ip"`
	Subnet string `bigquery:"ip_subnet"`
}

type SvcInfo struct {
	Ip      string `bigquery:"ipv4"`
	Service string `bigquery:"service"`
}

type CensysSvcInfo struct {
	Ip      string `bigquery:"ipv4"`
	Service string `bigquery:"svcname"`
}
type ProjectInfo struct {
	Project     string
	Dataset     string
	Table       string
	Credentials string
}

type DateInfo struct {
	Date 		 string `bigquery:"snapshot_date"`

}
type QueryType int

const (
	SUBNET  QueryType = 0
	SERVICE           = 1
	CENSYS			  = 2
)
