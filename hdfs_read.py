from pyspark.sql import SparkSession

def basic_read_write(spark):
	df = spark.read.load("examples/src/main/resources/userdata.parquet")
	df.write.save("namesAndFavColors.parquet")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Python Spark SQL data source example").getOrCreate()
    basic_read_write(spark)
    spark.stop()
