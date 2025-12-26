object tran_act {
  // Sample RDD
  val rdd = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10))

  // map
  val mapped = rdd.map(_ * 2)

  // filter
  val filtered = mapped.filter(_ > 10)

  // flatMap
  val wordsRDD = sc.parallelize(List("hello spark", "spark is fast"))
  val flatMapped = wordsRDD.flatMap(_.split(" "))

  // union
  val flatMapped = wordsRDD.flatMap(_.split(" "))

  // distinct
  val distinctRDD = sc.parallelize(List(1,2,2,3,3,3)).distinct()

  // sample
  val sampleRDD = rdd.sample(false, 0.3)

  // repartition (shuffle)
  val repartitioned = rdd.repartition(4)

  // coalesce (no shuffle)
  val coalesced = repartitioned.coalesce(2)

  // ACTIONS
  println(mapped.collect().toList)
  println(filtered.take(3).toList)
  println(flatMapped.collect().toList)
  println(unionRDD.count())
  println(distinctRDD.collect().toList)

}
