# Sample Data Setup (PySpark)
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("DateTimeFunctions").getOrCreate()

data = [
    ("2024-01-01", "2024-01-01 10:15:30"),
    ("2024-02-15", "2024-02-15 23:59:59"),
    ("2024-12-31", "2024-12-31 00:00:00")
]

df = spark.createDataFrame(data, ["date_str", "ts_str"])
df.show(truncate=False)

# Conversion Functions
# to_date()
df.select(to_date("date_str").alias("date")).show()

# With format:
df.select(to_date("date_str", "yyyy-MM-dd")).show()

# to_timestamp()
df.select(to_timestamp("ts_str").alias("timestamp")).show()


# With format:

df.select(to_timestamp("ts_str", "yyyy-MM-dd HH:mm:ss")).show()

# unix_timestamp()
df.select(unix_timestamp("ts_str").alias("unix_ts")).show()

# from_unixtime()
df.select(from_unixtime(unix_timestamp("ts_str")).alias("dt")).show()

# cast() (Most Recommended)
df.select(col("ts_str").cast("timestamp").cast("long").alias("unix_ts")).show()

# Current Date & Time
# current_date()
df.select(current_date().alias("current_date")).show()

# current_timestamp()
df.select(current_timestamp().alias("current_ts")).show()

# Date Arithmetic
# date_add()
df.select(date_add(to_date("date_str"), 7).alias("date_plus_7")).show()

# date_sub()
df.select(date_sub(to_date("date_str"), 10).alias("date_minus_10")).show()

# datediff()
df.select(datediff(current_date(), to_date("date_str")).alias("days_diff")).show()

#  months_between()
df.select(months_between(current_date(), to_date("date_str")).alias("months_diff")).show()

# add_months()
df.select(add_months(to_date("date_str"), 3).alias("plus_3_months")).show()

# Extracting Date Parts
# year()
df.select(year("date_str").alias("year")).show()

# month()
df.select(month("date_str").alias("month")).show()

# dayofmonth()
df.select(dayofmonth("date_str").alias("day")).show()

 # dayofweek()
df.select(dayofweek("date_str").alias("day_of_week")).show()

# dayofyear()
df.select(dayofyear("date_str").alias("day_of_year")).show()

# weekofyear()
df.select(weekofyear("date_str").alias("week")).show()

# Truncation & Formatting
# date_trunc()
df.select(date_trunc("month", to_timestamp("ts_str")).alias("month_trunc")).show()

 # trunc()
df.select(trunc(to_date("date_str"), "MM").alias("month_start")).show()

# date_format()
df.select(date_format("ts_str", "dd-MMM-yyyy").alias("formatted")).show()

# Timestamp Parts
#   hour()
df.select(hour("ts_str").alias("hour")).show()

#   minute()
df.select(minute("ts_str").alias("minute")).show()

# second()
df.select(second("ts_str").alias("second")).show()

# Timezone Handling (Important)
# to_utc_timestamp()
df.select(to_utc_timestamp("ts_str", "Asia/Kolkata")).show()

# from_utc_timestamp()
df.select(from_utc_timestamp("ts_str", "Asia/Kolkata")).show()

# Last / Next Dates
# last_day()
df.select(last_day("date_str").alias("month_end")).show()

# df.select(next_day("date_str", "Mon").alias("next_monday")).show()

 # Validation / Null Handling
 # isnull() / isnotnull()
df.select(col("date_str").isNull()).show()

# Spark SQL Equivalents (Example)
# SELECT
#   current_date(),
#   current_timestamp(),
#   unix_timestamp(ts_str),
#   date_add(date_str, 5),
#   datediff(current_date(), date_str),
#   year(date_str),
#   month(date_str),
#   date_format(ts_str, 'yyyy/MM/dd')
# FROM table_name;


# Category	Functions
# Conversion	to_date, to_timestamp, unix_timestamp, from_unixtime, cast
# Current	current_date, current_timestamp
# Arithmetic	date_add, date_sub, datediff, months_between, add_months
# Extract	year, month, dayofmonth, weekofyear, hour
# Format	date_format, trunc, date_trunc
# Timezone	to_utc_timestamp, from_utc_timestamp
#| Category          | Function             | Sample Code (PySpark)                           | Sample Output / Use Case      |
#| ----------------- | -------------------- | ----------------------------------------------- | ----------------------------- |
#| Conversion        | `to_date`            | `to_date(col("dt_str"))`                        | Converts string → `Date`      |
#| Conversion        | `to_timestamp`       | `to_timestamp(col("ts_str"))`                   | Converts string → `Timestamp` |
#| Conversion        | `unix_timestamp`     | `unix_timestamp(col("ts_str"))`                 | Timestamp → Unix seconds      |
#| Conversion        | `from_unixtime`      | `from_unixtime(col("unix_ts"))`                 | Unix → Timestamp              |
#| Conversion (Best) | `cast`               | `col("ts_str").cast("timestamp").cast("long")`  | Stable TS → Unix              |
#| Current           | `current_date`       | `current_date()`                                | Today’s date                  |
#| Current           | `current_timestamp`  | `current_timestamp()`                           | Current system timestamp      |
#| Arithmetic        | `date_add`           | `date_add(col("dt"), 7)`                        | Add 7 days                    |
#| Arithmetic        | `date_sub`           | `date_sub(col("dt"), 5)`                        | Subtract 5 days               |
#| Arithmetic        | `datediff`           | `datediff(current_date(), col("dt"))`           | Difference in days            |
#| Arithmetic        | `months_between`     | `months_between(current_date(), col("dt"))`     | Month difference              |
#| Arithmetic        | `add_months`         | `add_months(col("dt"), 2)`                      | Add 2 months                  |
#| Extract           | `year`               | `year(col("dt"))`                               | Extract year                  |
#| Extract           | `month`              | `month(col("dt"))`                              | Extract month                 |
#| Extract           | `dayofmonth`         | `dayofmonth(col("dt"))`                         | Extract day                   |
#| Extract           | `weekofyear`         | `weekofyear(col("dt"))`                         | Week number                   |
#| Extract           | `dayofweek`          | `dayofweek(col("dt"))`                          | Day index (1–7)               |
#| Time              | `hour`               | `hour(col("ts"))`                               | Extract hour                  |
#| Time              | `minute`             | `minute(col("ts"))`                             | Extract minute                |
#| Time              | `second`             | `second(col("ts"))`                             | Extract seconds               |
#| Format            | `date_format`        | `date_format(col("ts"), "dd-MMM-yyyy")`         | Custom date format            |
#| Truncate          | `trunc`              | `trunc(col("dt"), "MM")`                        | Month start                   |
#| Truncate          | `date_trunc`         | `date_trunc("month", col("ts"))`                | Month-level TS                |
#| Timezone          | `to_utc_timestamp`   | `to_utc_timestamp(col("ts"), "Asia/Kolkata")`   | Local → UTC                   |
#| Timezone          | `from_utc_timestamp` | `from_utc_timestamp(col("ts"), "Asia/Kolkata")` | UTC → Local                   |
#| Month End         | `last_day`           | `last_day(col("dt"))`                           | Last day of month             |
#| Navigation        | `next_day`           | `next_day(col("dt"), "Mon")`                    | Next Monday                   |


