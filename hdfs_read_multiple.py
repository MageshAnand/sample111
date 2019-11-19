import sys

from pyspark import SparkContext

def rddDev(sc):
 rddHadoop = sc.hadoopFile("hdfs:///tmp/HDFS/RDD/sample","org.apache.hadoop.mapred.TextInputFormat","org.apache.hadoop.io.Text","org.apache.hadoop.io.Text")
 rddHadoop.saveAsHadoopFile("hdfs:///tmp/HDFS/RDD/target","org.apache.hadoop.mapred.TextOutputFormat","org.apache.hadoop.io.Text","org.apache.hadoop.io.Text")

if __name__ == "__main__":
 # create Spark context with Spark configuration
 conf = SparkConf().setAppName("Read hadoop to RDD - Python")
 sc = SparkContext(conf)
 rddDev(sc)
