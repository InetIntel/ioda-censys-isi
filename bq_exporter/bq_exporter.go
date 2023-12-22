package main

import (
		bqutil "github.gatech.edu/antcensys/bqutil"
		"fmt"
		"sync"
		"github.com/golang/glog"
		"flag"
		"os"
		"google.golang.org/api/iterator"
		 "cloud.google.com/go/bigquery"

)


func initProjectInfo(srcTable string, srcDataset string,) bqutil.ProjectInfo{
	//creds_file := "censys-research-340718-7efa9b4a67c9.json"
	creds_file := "bq/censys/censys_key.json"
	srcProject := "censys-research-340718"
	//srcProject = "censys-io"
	return bqutil.ProjectInfo{Credentials:creds_file, Project: srcProject, Dataset: srcDataset, Table:srcTable}

}

func makeDateQuery(projectInfo bqutil.ProjectInfo, date string) {
	query := "SELECT DISTINCT snapshot_date FROM " + projectInfo.Dataset + "." + projectInfo.Table   + " WHERE snapshot_date <'" + date + "' AND snapshot_date > '2020-11-01'" 
	data, _ := bqutil.RunQuery(projectInfo, query)
	for {
		var row []bigquery.Value
		err := data.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			glog.Errorf("error iterating through results: %v", err)
		}
		fmt.Printf("%s\n", row[0])
	}

}

func makeQueries(projectInfo bqutil.ProjectInfo, queryType bqutil.QueryType, dataDir string, date string) {
	patterns := [...]string{ "1." ,"10.", "100", "101", "102",  "103", "104","105", "106", "107","108",  "109", "110", "11", "12", "13", "14", "15", "16", "17", "18", "19", "2", "3", "4", "5", "6", "7", "8", "9"}
//	patterns := [...]string{"10."}
	var wg sync.WaitGroup
	for _, p := range patterns {
		wg.Add(1)
		fmt.Println(p)
		go func(pattern string){
			defer wg.Done()
			glog.Info("querying for " + pattern )
			query := ""
			outputfile := dataDir + "/" + projectInfo.Dataset + "." + projectInfo.Table + "_" + "part_" +pattern + ".csv"
			if bqutil.SUBNET ==  queryType {
				query = bqutil.ConstructSubnetQuery(pattern, projectInfo)
				glog.Info(query, outputfile)
				results, err := bqutil.RunQuery(projectInfo, query)
				if err != nil {
					panic(err)
				} else {
					bqutil.ExportSubnetsToCSV(results, outputfile)
				}
			} else if bqutil.SERVICE == queryType{
				query = bqutil.ConstructSvcQuery(pattern, projectInfo)
				glog.Info(query, outputfile)
				results, err := bqutil.RunQuery(projectInfo, query)
				if err != nil {
					panic(err)
				} else {
					bqutil.ExportServicesToCSV(results, outputfile)
				}
			} else{
				query = bqutil.ConstructCensysSvcQuery(pattern, projectInfo, date)
				glog.Info(query, outputfile)
				results, err := bqutil.RunQuery(projectInfo, query)
				if err != nil {
					panic(err)
				} else {
					bqutil.ExportCensysServicesToCSV(results, outputfile)
				}
			}
		}(p)
	}
	wg.Wait()
}

func usage() {
	fmt.Fprintf(os.Stderr, "usage: example -stderrthreshold=[INFO|WARNING|FATAL] -log_dir=[string]\n", )
	flag.PrintDefaults()
	os.Exit(2)
}

func init() {
	flag.Usage = usage
	// NOTE: This next line is key you have to call flag.Parse() for the command line 
	// options or "flags" that are defined in the glog module to be picked up.
}
func main(){
	var tableName string
	var date string
	var queryType int
	var dataset string
	outputDir := "/data1/manasvini/censys"
	flag.StringVar(&tableName, "t", "snapshot_20210727", "table to query. Default is snapshot_20210727")
	/*
		NOTE:	
		tables in bq are either prefixed with snapshots_ or subnets_ followed by the date that the censys scan took place on. 
		snapshots has <ip, service name> pairs. subnets_ has <ip, /24> pairs. 

	*/
	flag.StringVar(&outputDir, "o", "/data1/manasvini/censys", "Output directory. Default is /data1/manasvini/censys")
	flag.IntVar(&queryType, "q", 0, "query type 0=SUBNETS 1=SERVICES default is SUBNETS")
	flag.StringVar(&dataset, "s", "censys_ips", "dataset name default is censys_ips")
	flag.StringVar(&date, "d", "2021-07-27", "date of snapshot")
	flag.Parse()
	defer glog.Flush()
	fmt.Println(tableName)
	projectInfo := initProjectInfo(tableName, dataset)
	if queryType == 0 {
		makeQueries(projectInfo, bqutil.SUBNET, outputDir, date)
	} else if queryType == 1{
		makeQueries(projectInfo, bqutil.SERVICE, outputDir, date)
	} else if queryType == 2{
		fmt.Println("Make censys query")
		makeQueries(projectInfo, bqutil.CENSYS, outputDir, date)
	}else {
		fmt.Println("date query")
		makeDateQuery(projectInfo, date)
	}
	


}
