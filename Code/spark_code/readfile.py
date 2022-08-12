from pyspark import SparkContext
from operator import countOf

strings= spark.read.text("file.txt")