from pyspark.sql import SparkSession

def jdbc_oracle_example1(spark):

       oracleDF = spark.read.jdbc("jdbc:sqlserver://esbcnprodcap02v.asg.com:1433;databaseName=pubs", "dbo.titles",properties={"user": "sa", "password": "qwer1234$","driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"})
       oracleDF.select("pub_id","title_id","advance").write.jdbc("jdbc:sqlserver://esbcnprodcap02v.asg.com:1433;databaseName=pubs", "dbo.business_tag",properties={"user": "sa", "password": "qwer1234$","driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"})

if __name__ == "__main__":
       spark = SparkSession.builder.appName("jdbc_api").getOrCreate()
       jdbc_oracle_example1(spark)
       spark.stop()
