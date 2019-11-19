from pyspark.sql import SparkSession

def basic_read_write(spark):
	df = spark.read.load("hdfs:///tmp/HDFS/userdata.parquet")
	df.write.save("hdfs:///tmp/HDFS/namesAndFavColors.parquet")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Python Spark SQL data source example").getOrCreate()
    basic_read_write(spark)
    spark.stop()
