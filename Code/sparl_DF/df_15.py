#3 show stored(highest to lowest) average grades in the second year

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,IntegerType,StringType,StructType
from pyspark.sql.functions import *
spark=SparkSession.builder.appName('Basics').getOrCreate()

data_schema = [StructField('student_id',StringType(),True),
                StructField('year',StringType(),True),
                StructField('sem1',StringType(),True),
                StructField('sem2',StringType(),True)]

final_struct=StructType(fields=data_schema)

df=spark.read.csv("student.txt",header=False, schema=final_struct)

filtered=df.filter(df['year']=='year2')
result_df= filtered.select('student_id','year', format_number(((df['sem1']+df['sem2'])/2),2).alias('AvgMks'))
result_df.orderBy(col('AvgMks').desc()).show()

#for top 3 students
result_df.orderBy(col('AvgMks').desc()).show(3)
