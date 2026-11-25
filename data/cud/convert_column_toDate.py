# Databricks notebook source
from pyspark.sql.functions import to_date

# Assuming the table name is "analytic_on_cloud.bi.auctions_dwh559_20230719_eomjune"
table_name = "ddd.default.auctions"

# Read the table into a DataFrame
df2 = spark.read.table(table_name)

# Show the updated DataFrame
display(df2)

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

# Convert the column "auction_date" from string to date using the Greek format "dd/MM/yyyy"
df2 = df2.withColumn("auction_date", to_date(df2.auction_date, "dd/MM/yyyy"))
df2 = df2.withColumn("publish_date", to_date(df2.publish_date, "dd/MM/yyyy HH:mm:ss"))


df2.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("ddd.default.auctions2")



# COMMAND ----------

from pyspark.sql.functions import to_date, lpad, col

# Assuming the table name is "analytic_on_cloud.bi.auctions_dwh559_20230719_eomjune"
table_name = "ddd.default.auctions"

# Read the table into a DataFrame
df5 = spark.read.table(table_name)

df5 = df5.withColumn("vat_number", lpad(col("vat_number").cast("string"), 9, "0"))

display(df5)




# COMMAND ----------


df5.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("ddd.default.auctions2")
