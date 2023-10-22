
# parquet experiments

# how to read

```scala

$ spark-shell

val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val parqFile = sqlContext.read.parquet("data_20231022-0930.parquet")
parqFile.registerTempTable("object")
val allRecords = sqlContext.sql("SELECT * FROM object")
allRecords.show()

```

taken from https://www.arm64.ca/post/reading-parquet-files-java/
