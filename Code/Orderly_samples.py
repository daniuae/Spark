from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("salary", IntegerType(), True)
])

# Variation 1: Explicit Schema using StructType

data = [(1, "Amit", 50000), (2, "Rahul", 60000)]

df = spark.createDataFrame(data, schema)
df.printSchema()

# Variation 2: Schema as DDL String (SQL-Style)
schema = "id INT, name STRING, salary INT"

df = spark.createDataFrame(data, schema)

# Variation 3: Infer Schema [NOT recommended for prod]
df = spark.createDataFrame(data, ["id", "name", "salary"])

#***********************************************************
# ALL VALID orderBy VARIATIONS
#***********************************************************
# Variation 1: Single Column Ascending (Default)
#***********************************************************

df.orderBy("salary")
df.orderBy(col("salary").asc())


# Variation 2: Single Column Descending

from pyspark.sql.functions import col

df.orderBy(col("salary").desc())


# Variation 3: Multiple Columns – Mixed Sorting
df.orderBy(
    col("salary").desc(),
    col("name").asc()
)


# Variation 4: Using sort() (Alias of orderBy)
df.sort("salary")
df.sort(col("salary").desc())

# Variation 5: Sorting with NULL handling
df.orderBy(col("salary").desc_nulls_last())

# Variation 6: Sorting Numeric Columns Stored as Strings

from pyspark.sql.functions import col

df.orderBy(col("salary").cast("int").desc())

