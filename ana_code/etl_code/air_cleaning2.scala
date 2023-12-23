// Drop unuseful columns in the data profiling process
df = df.drop("units","daily_obs_count","percent_complete","state","state_code","site_id","site_latitude","site_longitude","county_code","cbsa_code")

// Drop unnecessary columns
// AQS_PARAMETER_DESC and AQS_PARAMETER_CODE related to the distinct measurement conditions for monitoring PM2.5 levels
// Source is related to data source
// Since we are primarily interested in the PM2.5 levels themselves,
// the measurement conditions and sources are not directly relevant to the analysis
// we dropped the twos columns to focus on the key information
df = df.drop("source","aqs_parameter_desc", "aqs_parameter_code")

// Check the schema
df.printSchema

// Convert measure_date to Date type
df = df.withColumn("measure_date", to_date($"measure_date", "MM/dd/yyyy"))

// Convert int columns to Int type
val intColumns = Seq("poc","daily_mean_concentration", "daily_aqi_value")
intColumns.foreach(colName => {
  df = df.withColumn(colName, col(colName).cast("int"))
})
