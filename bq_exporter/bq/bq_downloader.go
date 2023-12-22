package bqutil

import (
	"cloud.google.com/go/bigquery"
	"context"
	"encoding/csv"
	"fmt"
	"github.com/golang/glog"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"os"
)

// exportTableAsCompressedCSV demonstrates using an export job to
// write the contents of a table into Cloud Storage as CSV.

func GetBqClient(credsFile string, projectName string) (*bigquery.Client, context.Context) {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectName, option.WithCredentialsFile(credsFile))
//, option.WithScopes("https://www.googleapis.com/auth/cloud-platform.read-only"))

	if err != nil {
		panic(err.Error())
	}
	return client, ctx
}

func ConstructSubnetQuery(ipPattern string, projectInfo ProjectInfo) string {
	query := "SELECT DISTINCT ip, ip_subnet" +
		" FROM `" + projectInfo.Project + "." + projectInfo.Dataset + "." + projectInfo.Table + "`" +
		" WHERE ip like '" + ipPattern + "%'"
	return query
}

func ConstructSvcQuery(ipPattern string, projectInfo ProjectInfo) string {
	query := "SELECT DISTINCT ipv4, service_name" +
		" FROM `" + projectInfo.Project + "." + projectInfo.Dataset + "." + projectInfo.Table + "`" +
		" WHERE ipv4 like '" + ipPattern + "%'"
	return query
}

func ConstructCensysSvcQuery(ipPattern string, projectInfo ProjectInfo, date string) string {
	query := "SELECT DISTINCT host_identifier.ipv4 as ipv4, svcs.service_name as svcname" +
		" FROM `" +  projectInfo.Dataset + "." + projectInfo.Table + "`,  UNNEST(services) svcs" +
		" WHERE host_identifier.ipv4 like '" + ipPattern + "%' AND  snapshot_date='" + date + "'"
	return query
}
func RunQuery(projectInfo ProjectInfo, query string) (*bigquery.RowIterator, error) {
	client, ctx := GetBqClient(projectInfo.Credentials, projectInfo.Project)
	defer client.Close()
	fmt.Println(query)
	glog.Info(query)

	data, err := client.Query(query).Read(ctx)
	return data, err

}

func writeCsv(records [][]string, filename string) {
	if len(records) == 0 {
		return
	}
	csvFile, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	fmt.Println(records[0][0], records[0][1])
	defer csvFile.Close()
	csvWriter := csv.NewWriter(csvFile)
	for _, r := range records {
		_ = csvWriter.Write(r)
	}
	csvWriter.Flush()
	glog.Infof("Wrote %d records\n", len(records))
}

func ExportCensysServicesToCSV(data *bigquery.RowIterator, outputFileName string) {
	var rows [][]string
	i := 0
	for {
		var row CensysSvcInfo
		err := data.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			glog.Errorf("error iterating through results: %v", err)
		}
		csvRow := make([]string, 2)
		csvRow[0] = row.Ip
		csvRow[1] = row.Service
		rows = append(rows, csvRow)
		i += 1
		if i%1000 == 0 {
			glog.Infof("Finished %d rows\n", i)
			glog.Infof("ip= %s svc = %s\n", row.Ip, row.Service)

		}
	}
	writeCsv(rows, outputFileName)
}
func ExportServicesToCSV(data *bigquery.RowIterator, outputFileName string) {
	var rows [][]string
	i := 0
	for {
		var row SvcInfo
		err := data.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			glog.Errorf("error iterating through results: %v", err)
		}
		csvRow := make([]string, 2)
		csvRow[0] = row.Ip
		csvRow[1] = row.Service
		rows = append(rows, csvRow)
		i += 1
		if i%1000 == 0 {
			glog.Infof("Finished %d rows\n", i)
//			fmt.Printf("ip= %s svc = %s\n", row.Ip, row.Service)
		}
	}
	writeCsv(rows, outputFileName)
}

func ExportSubnetsToCSV(data *bigquery.RowIterator, outputFileName string) {
	var rows [][]string
	i := 0
	for {
		var row IpInfo
		csvRow := make([]string, 2)
		err := data.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			glog.Errorf("%s, error iterating through results: %v", row, err)
		}
		csvRow[0] = row.Ip
		csvRow[1] = row.Subnet
		rows = append(rows, csvRow)
		i += 1
		if i%1000 == 0 {
			glog.Infof("Finished %d rows\n", i)
		}
//		fmt.Printf( "ip: %s /svc: %s\n", row.Ip, row.Subnet)
	}
	writeCsv(rows, outputFileName)
}

// [END bigquery_extract_table]
