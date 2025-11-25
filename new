from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dense_rank, rank
from pyspark.sql.window import Window
import os

spark = SparkSession.builder.appName("DataFrame").getOrCreate()

data = [(1,"Alice", "HR", 125000),
        (2,"Bob", "HR", 90000),
        (3,"Charlie", "IT", 80000),
        (4,"Dan", "IT", 85000),
        (5,"Eve", "IT", 75000),
        (6,"Frank", "HR", 80000)]

columns = ["id", "Name", "dept", "salary"]
df = spark.createDataFrame(data=data, schema=columns)

# Windows
x = Window.partitionBy("dept").orderBy(col("salary").desc())
y = Window.partitionBy("dept").orderBy(col("salary").asc())

# 2nd highest salary per dept
df2 = df.withColumn("dense_rank", dense_rank().over(x))
df_highest = df2.filter(col("dense_rank") == 2)

# Lowest salary per dept
df8 = df.withColumn("Lowest_Salary", rank().over(y))
df_lowest = df8.filter(col("Lowest_Salary") == 1)

df_highest.show()
df_lowest.show()
