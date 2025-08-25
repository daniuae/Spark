package com.Stream.Demo
package com.Stream.Demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object StreamingFC {
  val spark = SparkSession.builder
    .appName("FileStreamDemo")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val lines = spark.readStream
    .format("text")
    .load("C:/streaming/input")

  val words = lines.as[String].flatMap(_.split(" "))

  val counts = words.groupBy("value").count()

  val query = counts.writeStream
    .outputMode("complete")
    .format("console")
    .start()

  query.awaitTermination()


}
