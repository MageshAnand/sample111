import sys

from pyspark import SparkContext

def rddHadoopSequence(sc):
 rddData = sc.sequenceFile("hdfs:///tmp/HDFS/RDD/source", "org.apache.hadoop.io.Text", "org.apache.hadoop.io.DoubleWritable")
 rddData.saveAsSequenceFile("hdfs:///tmp/HDFS/RDD/target")

if __name__ == "__main__":
 # create Spark context with Spark configuration
 conf = SparkConf().setAppName("Read sequence to RDD - Python")
 sc = SparkContext(conf)
 rddHadoopSequence(sc)
