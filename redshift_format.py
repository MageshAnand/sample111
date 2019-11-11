from pyspark.sql import SparkSession
def jdbc_redshift_example(spark):

    # Reading from RedShift source
    redshiftDF = spark.read \
        .format("com.databricks.spark.redshift") \
        .option("url", "jdbc:redshift://aws-redshift.c3iskdv9vipb.us-east-1.redshift.amazonaws.com:5439/world?user=master&password=test123") \
        .option("dbtable", "demo.city") \
		.option("tempdir", "s3n://path/for/temp/data") \
        .load()

    # Saving data to a RedShift source
    redshiftDF.write \
        .format("com.databricks.spark.redshift") \
        .option("url", "jdbc:redshift://aws-redshift.c3iskdv9vipb.us-east-1.redshift.amazonaws.com:5439/world?user=master&password=test123") \
        .option("dbtable", "demo.city1") \
        .option("tempdir", "s3n://path/for/temp/data") \
		.mode("error") \
        .save()
        
if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("format_jdbc") \
        .getOrCreate()

    print("Format API")
    jdbc_redshift_example(spark)
    spark.stop()
