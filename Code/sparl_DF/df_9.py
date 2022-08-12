#use of alias for short variable names(change names)
from audioop import avg
from email.header import Header
from itertools import count
from pyspark.sql import SparkSession

#for the other equivalent syntax we need to import "functions" "countDistinct"(avg, sum,min,max,count)
from pyspark.sql.functions import countDistinct, avg, sum, min, max, count, format_number

spark=SparkSession.builder.appName('Basics').getOrCreate()

df = spark.read.csv("sales_info-2.csv", inferSchema=True, header=True)
sales_avg =df.select(avg('sales').alias('tempvar'))
sales_avg.select(format_number('tempvar',2).alias('Average')).show()