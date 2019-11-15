from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql import Row
 
def jdbc_subtract_example(spark):
    dep1DF = spark.read.format("jdbc").option("url", "jdbc:oracle:thin:@localhost:1521:orcl").option("driver","oracle.jdbc.driver.OracleDriver").option("dbtable", "COLLECTOR.DEPARTMENT1").option("user", "pyspark").option("password", "pyspark").load()
    CUSTDF = spark.read.format("jdbc").option("url", "jdbc:oracle:thin:@localhost:1521:orcl").option("driver","oracle.jdbc.driver.OracleDriver").option("dbtable", "COLLECTOR.CUSTOMERS").option("user", "pyspark").option("password", "pyspark").load()
    departmentDF = CUSTDF.subtract(dep1DF)
    
if __name__ == "__main__":
    spark = SparkSession.builder.appName("Python Spark SQL data source example").getOrCreate()
    jdbc_subtract_example(spark)
    spark.stop()
