## ioda-censys-isi  
This repository contains the scripts for creating and analyzing the datasets used in "Towards Improving Outage Detection with Multiple Probing  Protocols". We first construct the Censys datasets from various snaphots of [Censys Universal Internet Dataset](https://support.censys.io/hc/en-us/articles/360056063151). We use ANT IP history datasets (e.g., [this snapshot](https://ant.isi.edu/datasets/readmes/internet_address_history_it105w-20230926.README.txt) directly.   
### bq_exporter  
The scripts in this directory query Censys BigQuery using a Go binary and outputs CSV files corresponding to a particular date. The CSV files contain the IP address of the host, and the scanned protocol for which it responded.  
### analysis  
This directory contains the Jupyter notebooks for analyzing the collected data. ANT stores its data in the fsdb format, and Censys dumps are collected via the bq_exporter as CSV files. To be able to somewhat efficiently analyze them, we first convert both datasets to parquet form. Then, we perform the merging of the datasets in multiple steps. See the correspondging [README](analysis/README.md) for more details on what each notebook does.  
