var air = spark.read.option("header", "true").csv("hdfs://nyu-dataproc-m/user/zs2134_nyu_edu/project/air_quality_cleaned/*.csv")
var weather = spark.read.option("header", "true").csv("hdfs://nyu-dataproc-m/user/zs2134_nyu_edu/project/weather_cleaned/*.csv")

// check dataset
air.count() // 3818
weather.count() // 3887

air = air.withColumn("measure_date", to_date($"measure_date", "yyyy-MM-dd"))
air = air.withColumnRenamed("measure_date", "date")
weather = weather.withColumn("date", to_date($"date", "yyyy-MM-dd"))

val mergedDF = air.join(weather, Seq("date"), "inner")
mergedDF.show()
mergedDF.printSchema()
mergedDF.count // 3732

// Uncomment if file is not created
// mergedDF.write.option("header", "true").csv("hdfs://nyu-dataproc-m/user/zs2134_nyu_edu/project/merged")
