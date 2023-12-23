// read dataframe
val db = spark.sql("use zs2134_nyu_edu")
val table = spark.sql("show tables")
println("Current tables in zs2134 databases:")
table.show()

var df = spark.sql("select * from air_quality") // read from Hive table
