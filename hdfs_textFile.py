import sys

from pyspark import SparkContext

def rddText(sc):
 # read input text file to RDD
 lines = sc.textFile("hdfs://PySpark/source.txt")
 lines.saveAsTextFile("hdfs://PySpark/target.txt")

if __name__ == "__main__":
 # create Spark context with Spark configuration
 conf = SparkConf().setAppName("Read Text to RDD - Python")
 sc = SparkContext(conf)
 rddText(sc)