import sys

from pyspark import SparkContext

def msSqlWrite(sc):
 sqlDF = sc.sequenceFile("hdfs:///tmp/HDFS/RDD/source", "org.apache.hadoop.io.Text", "org.apache.hadoop.io.DoubleWritablee")
 sqlDF.saveAsSequenceFile("hdfs:///tmp/HDFS/RDD/target")

if __name__ == "__main__":
 # create Spark context with Spark configuration
 conf = SparkConf().setAppName("Read sequence to RDD - Python")
 sc = SparkContext(conf)
 msSqlWrite(sc)
