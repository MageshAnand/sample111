from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql import Row
 
def jdbc_subtract_example(spark):
    adult1DF = spark.read.format("jdbc").option("url", "jdbc:oracle:thin:@localhost:1521:orcl").option("driver","oracle.jdbc.driver.OracleDriver").option("dbtable", "COLLECTOR.ADULTS").option("user", "pyspark").option("password", "pyspark").load()
    adult2DF = spark.read.format("jdbc").option("url", "jdbc:oracle:thin:@localhost:1521:orcl").option("driver","oracle.jdbc.driver.OracleDriver").option("dbtable", "COLLECTOR.ADULTS2").option("user", "pyspark").option("password", "pyspark").load()
    writeDF = adult1DF.subtract(adult2DF)
    
if __name__ == "__main__":
    spark = SparkSession.builder.appName("Python Spark SQL data source example").getOrCreate()
    jdbc_subtract_example(spark)
    spark.stop()
