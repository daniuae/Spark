from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# We’ll use this base DataFrame throughout.

spark = SparkSession.builder.getOrCreate()

data = [
    ("2024-01-15", "2024-01-15 10:30:45"),
    ("2023-12-01", "2023-12-01 23:59:59"),
    ("2022-07-20", "2022-07-20 05:10:15")
]

df = spark.createDataFrame(data, ["date_str", "ts_str"])
df.show(truncate=False)

# CURRENT DATE & TIME FUNCTIONS
# current_date()
df.select(current_date().alias("today")).show()

# current_timestamp()
df.select(current_timestamp().alias("now")).show()

# STRING ➜ DATE / TIMESTAMP (CASTING & CONVERSION)
# to_date() – String → Date
df.withColumn("date", to_date("date_str")).show()
# With format:
to_date(col("date_str"), "yyyy-MM-dd")

# to_timestamp() – String → Timestamp
df.withColumn("timestamp", to_timestamp("ts_str")).show()
# With format:
to_timestamp(col("ts_str"), "yyyy-MM-dd HH:mm:ss")

# CASTING DATE & TIMESTAMP (USING cast())

# String → Date
df.withColumn("date", col("date_str").cast("date")).show()

# String → Timestamp
df.withColumn("timestamp", col("ts_str").cast("timestamp")).show()

# Date → String
df.withColumn("date_str", col("date_str").cast("string")).show()

# DATE FORMATTING
# date_format()
df.withColumn(
    "formatted_date",
    date_format(to_date("date_str"), "dd-MMM-yyyy")
).show()
# Formats:
# yyyy
# MM
# dd
# HH
# mm
# ss
# MMM

# UNIX TIMESTAMP FUNCTIONS
# 5 unix_timestamp() – Timestamp → Epoch seconds
df.withColumn(
    "epoch",
    unix_timestamp("ts_str", "yyyy-MM-dd HH:mm:ss")
).show()

# Current Unix Timestamp
df.select(unix_timestamp().alias("current_epoch")).show()
# Epoch → Timestamp

df.withColumn(
    "from_epoch",
    from_unixtime(unix_timestamp("ts_str", "yyyy-MM-dd HH:mm:ss"))
).show()

# With format:
from_unixtime(col("epoch"), "yyyy-MM-dd HH:mm:ss")
# 6 DATE ARITHMETIC FUNCTIONS
# date_add() / date_sub()

df.withColumn("plus_7_days", date_add("date_str", 7)) \
  .withColumn("minus_10_days", date_sub("date_str", 10)) \
  .show()
add_months()
df.withColumn("plus_2_months", add_months("date_str", 2)).show()
# months_between()

df.select(
    months_between(lit("2024-01-01"), col("date_str")).alias("months_diff")
).show()
# datediff()

df.select(
    datediff(current_date(), to_date("date_str")).alias("days_diff")
).show()
# 7 DATE EXTRACTION FUNCTIONS
# Year / Month / Day

df.select(
    year("date_str").alias("year"),
    month("date_str").alias("month"),
    dayofmonth("date_str").alias("day")
).show()

# Week & Day Info
df.select(
    weekofyear("date_str").alias("week"),
    dayofweek("date_str").alias("day_of_week"),
    dayofyear("date_str").alias("day_of_year")
).show()
# 8 START / END OF PERIOD FUNCTIONS
# last_day()

df.withColumn("month_end", last_day("date_str")).show()
# trunc() – Truncate to month/year
df.select(
    trunc("date_str", "MM").alias("month_start"),
    trunc("date_str", "YYYY").alias("year_start")
).show()

# 9 TIMESTAMP DIFFERENCE & EXTRACTION
# Hour / Minute / Second
df.select(
    hour("ts_str").alias("hour"),
    minute("ts_str").alias("minute"),
    second("ts_str").alias("second")
).show()

# TIME ZONE CONVERSIONS
# 10 from_utc_timestamp()
df.withColumn(
    "ist_time",
    from_utc_timestamp("ts_str", "Asia/Kolkata")
).show()

# to_utc_timestamp()

df.withColumn(
    "utc_time",
    to_utc_timestamp("ts_str", "Asia/Kolkata")
).show()

# COALESCE & NULL HANDLING
df.withColumn(
    "safe_date",
    coalesce(to_date("date_str"), current_date())
).show()

# 12 COMMON INTERVIEW / CERTIFICATION PITFALLS
# REAL-WORLD EXAMPLE (ETL READY)
df_final = df \
    .withColumn("event_date", to_date("ts_str")) \
    .withColumn("event_epoch", unix_timestamp("ts_str")) \
    .withColumn("year", year("ts_str")) \
    .withColumn("month", month("ts_str")) \
    .withColumn("load_date", current_date())

df_final.show(truncate=False)
