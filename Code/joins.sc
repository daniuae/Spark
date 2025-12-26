val df1 = Seq(
  (1,"A"),
  (2,"B"),
  (3,"C")
).toDF("id","name")

val df2 = Seq(
  (1,"HR"),
  (2,"IT")
).toDF("id","dept")

// Inner Join
df1.join(df2, "id", "inner").show()

// Left Anti Join (EXAM FAVORITE)
df1.join(df2, "id", "left_anti").show()
