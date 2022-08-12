#chapter4 Q1) Display all the customers in San Joes whose rating is above 200

from pyspark.sql import SparkSession

spark=SparkSession.builder.appName('Chapter3').getOrCreate()

csv_file="customers.txt"

df=(spark.read.format("csv").option("inferSchema","true").option("header","true").load(csv_file))
df.createOrReplaceTempView("cust_table")

spark.sql("""SELECT * FROM cust_table 
    WHERE city='San Jose' and rating > 200""").show()
df.filter((df.city=="San Jose")&(df.rating>100)).show()
