from pyspark.sql import SparkSession 
spark= SparkSession.builder.appName('Basics').getOrCreate()
empDF=spark.read.csv('employee.csv', inferSchema=True, header=True)
deptDF=spark.read.csv('department.csv', inferSchema=True, header=True)

empDF.join(deptDF, empDF.emp_dept_id == deptDF.emp_dept_id).show()

