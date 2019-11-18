from pyspark.sql import SparkSession

def hdfs_field_select(spark):
	df = spark.read.csv("hdfs:///tmp/HDFS/country.csv")
	df.select("_c1").write.csv("hdfs:///tmp/HDFS/city.csv")
	
if __name__ == "__main__":
    spark = SparkSession.builder.appName("Python Spark SQL data source example").getOrCreate()
    hdfs_field_select(spark)
    spark.stop()
