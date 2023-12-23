// read dataframe
val db = spark.sql("use zs2134_nyu_edu")
val table = spark.sql("show tables")
println("Current tables in zs2134 databases:")
table.show()

var df = spark.sql("select * from air_quality") // read from Hive table

// Display the schema of the DataFrame
println("DataFrame Schema for Air Quality Dataset:")
df.printSchema()

// Check number of records
println("Number of records:")
df.count() // 129546

// Drop headers
df.show(5) // found the table contains the header, filter the header
df = df.filter(col("measure_date") !== "Date")
df.count() // 129535

// Handle Missing Values. Result: no missing values
val na_dropped = df.na.drop()
println(s"There are ${df.count() - na_dropped.count()} rows contain NaN values")

// Handle Duplicate Rows. Result: no duplicate rows
val duplicate_dropped = df.dropDuplicates()
println(s"There are ${df.count() - duplicate_dropped.count()} duplicate rows")
