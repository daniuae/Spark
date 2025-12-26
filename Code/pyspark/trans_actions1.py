from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

df = spark.createDataFrame([
    (1,"A",1000),
    (2,"B",2000),
    (3,"A",3000),
    (4,"B",1500)
], ["id","dept","salary"])

df.select("dept","salary").show()
df.filter(col("salary") > 1500).show()
df.withColumn("bonus", col("salary") * 0.1).show()
df.groupBy("dept").avg("salary").show()
df.orderBy(col("salary").desc()).show()

print(df.count())
