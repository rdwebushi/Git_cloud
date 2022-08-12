from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,IntegerType,StringType,StructType
from pyspark.sql.functions import format_number
spark=SparkSession.builder.appName('Basics').getOrCreate()

data_schema = [StructField('student_id',StringType(),True),
                StructField('year',StringType(),True),
                StructField('sem1',StringType(),True),
                StructField('sem2',StringType(),True)]

final_struct=StructType(fields=data_schema)

df=spark.read.csv("student.txt",header=False, schema=final_struct)

filtered= df.filter(df['year']=='year2').select(['student_id','year','sem1','sem2']).collect()

result_df= df.select('student_id','year', format_number(((df['sem1']+df['sem2'])/2),2).alias('AvgMks'))
result_df.show()

