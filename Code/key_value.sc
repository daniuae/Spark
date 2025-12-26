val kvRDD = sc.parallelize(List(
  ("apple", 1),
  ("banana", 2),
  ("apple", 3)
))

// reduceByKey (BEST)
val reduced = kvRDD.reduceByKey(_ + _)

// groupByKey (AVOID)
val grouped = kvRDD.groupByKey()

// mapValues
val mappedValues = kvRDD.mapValues(_ * 10)

// sortByKey
val sorted = kvRDD.sortByKey()

// ACTION
reduced.collect().foreach(println)
