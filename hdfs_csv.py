from pyspark.sql import SparkSession

def basic_datasource_example_na(spark):
	df = spark.read.csv("hdfs://tmp/HDFS/country.csv")
	df.write.csv("hdfs://tmp/HDFS/city.csv")
	
if __name__ == "__main__":
    spark = SparkSession.builder.appName("Python Spark SQL data source example").getOrCreate()
    basic_datasource_example_na(spark)
    spark.stop()
