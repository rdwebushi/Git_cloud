from pyspark.sql import SparkSession
spark=(SparkSession.builder.appName('SparkSql Demo')).getOrCreate()
csv_file="sales_info-2.csv"

df=(spark.read.format("csv").option("inferSchema","true").option("header","true").load(csv_file))

df.createOrReplaceTempView("sales_table")

spark.sql("""SELECT Company, AVG(Sales) 
    FROM sales_table
    GROUP BY Company""").show()