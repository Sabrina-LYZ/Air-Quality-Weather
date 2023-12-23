# Air-Quality-Weather

# **Air Quality and Weather Data Analysis Project**

This project is divided into three main parts, each focusing on a specific aspect of the analysis:

1. air quality dataset cleaning, profiling, and analysis
2. weather dataset cleaning, profiling, and analysis
3. integrated dataset analysis with a regression task

The current repository contains code written by the repository owner for part1 air quality dataset cleaning, profiling, and analysis, and part3 integrated dataset analysis with a regression task.

****Directory Structure****

```scala
ana_code/
|-- data/
|   |-- 2013_ad_viz_plotval_data.csv
|   |-- 2014_ad_viz_plotval_data.csv
|   |-- ...
|
|-- data_ingest/
|   |-- store_air_quality.txt (store dataset into Hive table)
|   |-- read_air_quality.scala (read Hive table into Spark)
|
|-- etl_code/
|   |-- air_cleaning1.scala
|   |-- ...
|
|-- profiling_code/
|   |-- air_profiling1.scala
|   |-- ...
|   |
|
|-- screenshots/
|   |-- air_quality
|   |-- merged
|
|-- README.md
```

## **Parts of the Project**

### **1. Air Quality Dataset Cleaning, Profiling, and Analysis**

- **Data Location:** Original air quality data files are stored in the `/data` directory. These datasets are uploaded to HDFS `hdfs://nyu-dataproc-m/user/zs2134_nyu_edu/project/air_quality`
- **Ingestion:** The ingestion process is in `/data_ingest` ****directory**.**
    - Please run in Hive with commands in `store_air_quality.txt` first
    - then run in Spark with `read_air_quality.scala` code
- **Cleaning and Profiling:** The data cleaning code is in the `/etl_code` directory. The data profiling code is in the `/profiling_code` directory.
    - Since the data cleaning and data profiling process are intertwined, please run in the following order:
        - `air_cleaning1.scala`
        - `air_profiling1.scala`
        - `air_cleaning2.scala`
        - `air_profiling2.scala`
    - You can directly see the results of each step in the Scala interactive shell
- **Analysis:** The code for analyzing air quality data is in `/analysis_code`
    - run `/air_analysis.scala` first
    - then run `air_cleaning3.scala` in the `/etl_code` directory
        - to further clean the dataset after analysis
        - please uncomment the `orange_avg.write.option` in the last line if the output file has not been generated in the HDFS system yet
- **Results:** Find the visualization results of air quality analysis in the `/screenshot` directory.

### **3. Integrated Dataset Analysis with Regression Task**

- code can be found in `/analysis_code` directory
    - run `merge.scala` to merge two datasets
        - please uncomment the `mergedDF.write.option` in the last line if the output file has not been generated in the HDFS system yet
    - run `regression.scala` for multiple linear regression task
- **Results:** Find the visualization and screenshot in the screenshot/merged directory
