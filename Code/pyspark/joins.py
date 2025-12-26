df1 = spark.createDataFrame([
    (1,"A"),
    (2,"B"),
    (3,"C")
], ["id","name"])

df2 = spark.createDataFrame([
    (1,"HR"),
    (2,"IT")
], ["id","dept"])

df1.join(df2, "id", "inner").show()
df1.join(df2, "id", "left_anti").show()
