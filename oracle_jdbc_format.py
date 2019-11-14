from __future__ import print_function

from pyspark.sql import SparkSession
from pyspark.sql import Row

def jdbc_dataset_example(spark):
    df1 = spark.read.format("jdbc").option("url", "jdbc:oracle:thin:@localhost:1521:orcl").option("driver","oracle.jdbc.driver.OracleDriver").option("dbtable", "COLLECTOR.PEOPLE").option("user", "pyspark").option("password", "pyspark").load()
    df2 = spark.read.format("jdbc").option("url", "jdbc:oracle:thin:@localhost:1521:orcl").option("driver","oracle.jdbc.driver.OracleDriver").option("dbtable", "COLLECTOR.ADULTS").option("user", "pyspark").option("password", "pyspark").load()
    jdbcDF = df1.intersectAll(df2)
    
if __name__ == "__main__":
    spark = SparkSession.builder.appName("Python Spark SQL data source example").getOrCreate()
    jdbc_dataset_example(spark)
    spark.stop()
