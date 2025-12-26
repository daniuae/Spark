import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

val windowSpec = Window.partitionBy("dept").orderBy($"salary".desc)

df.withColumn("rank", rank().over(windowSpec)).show()

