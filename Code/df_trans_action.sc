import spark.implicits._

val df = Seq(
  (1,"A",1000),
  (2,"B",2000),
  (3,"A",3000),
  (4,"B",1500)
).toDF("id","dept","salary")

// select
df.select("dept","salary").show()

// filter / where
df.filter($"salary" > 1500).show()

// withColumn
df.withColumn("bonus", $"salary" * 0.1).show()

// groupBy
df.groupBy("dept").avg("salary").show()

// orderBy
df.orderBy($"salary".desc).show()

// ACTIONS
println(df.count())
df.collect()
