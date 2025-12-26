-- Run the following 
--Type pyspark from command prompt 

rdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10])

mapped = rdd.map(lambda x: x * 2)
filtered = mapped.filter(lambda x: x > 10)

words = sc.parallelize(["hello spark", "spark is fast"])
flatmapped = words.flatMap(lambda x: x.split(" "))

union_rdd = rdd.union(sc.parallelize([11,12]))
distinct_rdd = sc.parallelize([1,2,2,3,3]).distinct()

sample_rdd = rdd.sample(False, 0.3)

print(mapped.collect())
print(filtered.take(3))
print(flatmapped.collect())
print(union_rdd.count())
print(distinct_rdd.collect())


