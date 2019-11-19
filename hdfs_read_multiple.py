from pyspark.sql import SparkSession

def copyToTarget(spark):
	df = spark.read.load("hdfs:///tmp/HDFS/source.parquet")
	df.select("name").write.save("hdfs:///tmp/HDFS/parquet/target1.parquet")
	df1 = spark.read.load("hdfs:///tmp/HDFS/parquet/source.parquet")
	df1.select("name", "favorite_color").write.save("hdfs:///tmp/HDFS/parquet/target2.parquet")
	
if __name__ == "__main__":
    spark = SparkSession.builder.appName("Python Spark SQL data source example").getOrCreate()
    copyToTarget(spark)
    spark.stop()
