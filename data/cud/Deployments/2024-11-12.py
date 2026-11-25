# Databricks notebook source
# MAGIC %md
# MAGIC #### DBTC-373
# MAGIC #### Add fileds PaymentTypeGroup, ProfitsPaymentTypeEng in collection primary table
# MAGIC
# MAGIC ### Steps 
# MAGIC
# MAGIC * Add PaymentTypeGroupMapping.csv file in the snapspot S3 path (s3://p-ec1-cepal-landing-zone/incoming/snapshot-files/in/DSE_REFERENCE) (find the [file] (https://eu-central-1.console.aws.amazon.com/s3/object/qa-ec1-cepal-landing-zone?region=eu-central-1&bucketType=general&prefix=incoming/snapshot-files/processed/DSE_REFERENCE/PaymentTypeGroupMapping.csv) as in dev, qa envs) in order to create table `p-cdh`.reference.paymenttypegroupmapping
# MAGIC * Run the following code in order to add new columns and populate fields

# COMMAND ----------

env = 'p'

# Add columns in Unity
spark.sql(f'''
    ALTER TABLE `{env}-cdh`.`primaryactive`.collection
    ADD COLUMNS (
        ProfitsPaymentTypeEng STRING AFTER ProfitsPaymentType,
        PaymentTypeGroup STRING AFTER ProfitsPaymentTypeEng
    )
''')

# COMMAND ----------

# Update new fields
paymenttypegroupmapping = spark.sql(f"""
    select distinct *
    from `{env}-cdh`.reference.paymenttypegroupmapping
    """)
paymenttypegroupmapping.createOrReplaceTempView("paymenttypegroupmapping")


profits_params = spark.sql(f"""
    select distinct serial_num, description_orig, businessdate
    from `{env}-cdh`.landed.profits_params
    where param_source = 'GD'
    and param_type = 'EXTYP'
    """)
profits_params.createOrReplaceTempView("profits_params")

collection = spark.sql(f"""
    select distinct ProfitsPaymentTypeCode, businessdate
    from `{env}-cdh`.`primaryactive`.collection
    """)
collection.createOrReplaceTempView("collection")
 
joined = spark.sql(f"""
                   select a.ProfitsPaymentTypeCode, b.PaymentTypeGroup, c.description_orig, a.businessdate
                   from collection a
                   left join paymenttypegroupmapping b
                   on a.ProfitsPaymentTypeCode = b.ProfitsPaymentTypeCode
                   left join profits_params c
                   on a.ProfitsPaymentTypeCode = c.serial_num and a.businessdate = c.businessdate
            """)
joined.createOrReplaceTempView("joined")
 
spark.sql(f"""
MERGE INTO `{env}-cdh`.`primaryactive`.collection a
USING joined b
ON a.ProfitsPaymentTypeCode = b.ProfitsPaymentTypeCode
    AND a.businessdate = b.businessdate
WHEN MATCHED THEN UPDATE SET a.PaymentTypeGroup = b.PaymentTypeGroup
                            , a.ProfitsPaymentTypeEng = b.description_orig
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### *D360-2042*: Add RequestCategoryType column in TicketingRequest table
# MAGIC * Run the following code in qa, prod in a notebook.
# MAGIC * Re-run TicketingRequest for businessdates 2024-11-04 and on in production.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE `p-cdh`.primaryactive.ticketingrequest
# MAGIC ADD COLUMN `RequestCategoryType` STRING AFTER `WorkflowModifiedDate`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## WE NEED HIVE-METASTORE CLUSTER FOR THIS

# COMMAND ----------

# Add columns in Hive
env="p"
spark.sql(f'''
    ALTER TABLE hive_metastore.`{env}-ec1-cepal-curated-area`.collection
    ADD COLUMNS (
        ProfitsPaymentTypeEng STRING AFTER ProfitsPaymentType,
        PaymentTypeGroup STRING AFTER ProfitsPaymentTypeEng
    )
''')

# COMMAND ----------

spark.sql(f'''
    ALTER TABLE hive_metastore.`{env}-ec1-cepal-curated-area`.collection
    ADD COLUMNS (
        ProfitsPaymentTypeEng STRING AFTER ProfitsPaymentType,
        PaymentTypeGroup STRING AFTER ProfitsPaymentTypeEng
    )
''')

# COMMAND ----------

spark.sql(f'''
    ALTER TABLE hive_metastore.`{env}-ec1-cepal-curated-area`.TicketingRequest
    ADD COLUMNS (
        RequestCategoryType STRING AFTER WorkflowModifiedDate
    )
''')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Re-run CUP Tables 
# MAGIC payment_arotron , ticketing_requsest_ssp for businessdate 2024-11-04,2024-11-05,2024-11-06,2024-11-07,2024-11-08,

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update all the views affected !! 
# MAGIC Collections,ticketingrequest
