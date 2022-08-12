from distutils import text_file
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName('Chapter3').getOrCreate()

csv_file="salespeople.txt"
#if your file do not have then use
#schema= "`snum` INT, `sname` STRING, `city` STRING,`comm` FLOAT"
df=(spark.read.format("csv").option("inferSchema","true").option("header","true").load(csv_file))
df.createOrReplaceTempView("cust_table")

spark.sql("""SELECT * FROM cust_table WHERE city==('London')""").show()
