from pyspark.sql import SparkSession

def basic_parquet_file(spark):
	df = spark.read.parquet("hdfs:///tmp/HDFS/userdata.parquet")
	df.write.parquet("hdfs:///tmp/HDFS/namesAndFavColors.parquet")
	
if __name__ == "__main__":
    spark = SparkSession.builder.appName("Python Spark SQL data source example").getOrCreate()
    basic_parquet_file(spark)
    spark.stop()
