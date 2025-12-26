kv = sc.parallelize([
    ("apple",1),
    ("banana",2),
    ("apple",3)
])

reduced = kv.reduceByKey(lambda a,b: a+b)
print(reduced.collect())

