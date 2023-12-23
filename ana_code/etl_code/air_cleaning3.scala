
// make sure each year has Orange county records
df.filter(df("county")==="Orange").groupBy("year").count().show() 
var orange = df.filter(df("county")==="Orange")

println("Dataset size after filtering with Orange county")
orange.count() // 4974

// For orange county, all measurements took place -> dropped
orange.select("site_name","cbsa_name","county").distinct.show() 

// we want to prediction daily_mean_concentration with weather dataset later
// since daily_mean_concentration and daily_aqi_value has high correlation
// we drop daily_aqi_value as well 
// we then dropped year and month cols
orange = orange.drop("site_name","cbsa_name","county","daily_aqi_value","year","month")
orange.printSchema


// find average, several measurements per day
orange.groupBy("measure_date").count().show() // -> multiple measurements per day(for different POC), we take the average
orange.groupBy("measure_date").agg(count("daily_mean_concentration"),(max("daily_mean_concentration")-min("daily_mean_concentration")).alias("range"), avg("daily_mean_concentration")).show()

var orange_avg = orange.groupBy("measure_date").agg(avg("daily_mean_concentration"))
orange_avg = orange_avg.withColumnRenamed("avg(daily_mean_concentration)","daily_mean_concentration")
orange_avg.printSchema()
orange_avg.count() // 3818
orange_avg.show()

// write output: uncomment this if file not created yet
// orange_avg.write.option ("header", "true").csv ("hdfs://nyu-dataproc-m/user/zs2134_nyu_edu/project/air_quality_cleaned")