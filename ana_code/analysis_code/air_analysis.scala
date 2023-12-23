
// Correlation Analysis
df.stat.corr("daily_mean_concentration", "daily_aqi_value")


// Extract year and month from 'measure_date'
df = df.withColumn("year", year($"measure_date"))
df = df.withColumn("month", month($"measure_date"))

// Seasonal Analysis
val seasonalAnalysis = df.groupBy("month").agg(avg("daily_mean_concentration").alias("avg_pm25"))
seasonalAnalysis.orderBy("month").show()

// Year Analysis
val yearAnalysis = df.groupBy("year").agg(avg("daily_mean_concentration").alias("avg_pm25"))
yearAnalysis.orderBy("year").show()

// 2023
df.filter(df("year") === 2023).groupBy("month").agg(avg("daily_mean_concentration").alias("avg_pm25")).show()
// Smoke plumes from Canada's most destructive wildfire season

// Geospatial Analysis
val geoAnalysis = df.groupBy("county").agg(avg("daily_mean_concentration").alias("avg_pm25"))
geoAnalysis.show()
