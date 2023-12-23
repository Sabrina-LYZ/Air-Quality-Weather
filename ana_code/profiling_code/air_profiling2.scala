// Check number of records
println("# of records in air quality dataset")
df.count() // 129535

// Check date range 2013-01-01 - 2023-11-20
df.select(min("measure_date"), max("measure_date")).show()

// Summary Statistics
df.select("daily_mean_concentration", "daily_aqi_value").summary().show()