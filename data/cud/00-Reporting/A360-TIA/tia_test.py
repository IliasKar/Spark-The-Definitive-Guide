# Databricks notebook source
# DBTITLE 1,Always Clone
env_prefix = "p"
dev_env_prefix = "d"


spark.sql(
     f"""
    DROP TABLE IF EXISTS `{dev_env_prefix}-cdh`.derivativeactive.DAuction360Matching 
    """
)

spark.sql(
        f"""
    CREATE OR REPLACE TABLE `{dev_env_prefix}-cdh`.derivativeactive.DAuction360Matching 
    DEEP CLONE `{env_prefix}-cdh`.derivativeactive.DAuction360Matching;
    """
)

spark.sql(
     f"""
    DROP TABLE IF EXISTS `d-cdh`.reference.auction_acquisition_by
    """
)


spark.sql(f"""
             CREATE OR REPLACE TABLE `d-cdh`.reference.auction_acquisition_by AS
    SELECT *
    FROM `p-cdh`.reference.auction_acquisition_by
    WHERE ReferenceDate = (
        SELECT MAX(ReferenceDate) FROM `p-cdh`.reference.auction_acquisition_by
    );
    """)

tables = [
    "primaryactive.party",
    "primaryactive.RealEstateInfo",
    "primaryactive.RealEstateBuilding",
    "primaryactive.collateral",
    "primaryactive.legalaction",
    "primaryactive.exposure",
    "derivativeactive.danalyticsmetrics"
]

deep_clone_queries = [
    f"""
    CREATE OR REPLACE TABLE `{dev_env_prefix}-cdh`.{table} AS
    SELECT *
    FROM `{env_prefix}-cdh`.{table}
    WHERE BusinessDate = (
        SELECT MAX(BusinessDate) FROM `{env_prefix}-cdh`.{table}
    );
    """
    for table in tables
]

drop_table_queries = [
    f"DROP TABLE IF EXISTS `{dev_env_prefix}-cdh`.{table};"
    for table in tables
]

for query in drop_table_queries + deep_clone_queries:
    print(query)
    spark.sql(query)

# COMMAND ----------

# DBTITLE 1,only deep clone
drop_table_queries = [
    "DROP TABLE IF EXISTS `d-cdh`.landing.newsphone_auctions;",
    "DROP TABLE IF EXISTS `d-cdh`.landed.newsphone_auctions;",
    "DROP TABLE IF EXISTS `d-cdh`.landing.newsphone_basic_data;",
    "DROP TABLE IF EXISTS `d-cdh`.landed.newsphone_basic_data;"
]

deep_clone_queries = [
    "CREATE OR REPLACE TABLE `d-cdh`.landing.newsphone_basic_data DEEP CLONE `p-cdh`.landing.newsphone_basic_data;",
    "CREATE OR REPLACE TABLE `d-cdh`.landed.newsphone_basic_data DEEP CLONE `p-cdh`.landed.newsphone_basic_data;",
    "CREATE OR REPLACE TABLE `d-cdh`.landing.newsphone_auctions DEEP CLONE `p-cdh`.landing.newsphone_auctions;",
    "CREATE OR REPLACE TABLE `d-cdh`.landed.newsphone_auctions DEEP CLONE `p-cdh`.landed.newsphone_auctions;"
]



for query in drop_table_queries + deep_clone_queries:
    print(query)
    spark.sql(query)

# COMMAND ----------

# DBTITLE 1,only primary derivative deep clone
drop_table_queries = [
    "DROP TABLE IF EXISTS `d-cdh`.primaryactive.auctionrealestate;", 
    "DROP TABLE IF EXISTS `d-cdh`.derivativeactive.dauctionrealestate;",
    "DROP TABLE IF EXISTS `d-cdh`.primaryactive.auction;", 
    "DROP TABLE IF EXISTS `d-cdh`.derivativeactive.dauction;"
]

deep_clone_queries = [
    "CREATE OR REPLACE TABLE `d-cdh`.primaryactive.auctionrealestate DEEP CLONE `p-cdh`.primaryactive.auctionrealestate;", 
    "CREATE OR REPLACE TABLE `d-cdh`.derivativeactive.dauctionrealestate DEEP CLONE `p-cdh`.derivativeactive.dauctionrealestate;",
    "CREATE OR REPLACE TABLE `d-cdh`.primaryactive.auction DEEP CLONE `p-cdh`.primaryactive.auction;", 
    "CREATE OR REPLACE TABLE `d-cdh`.derivativeactive.dauction DEEP CLONE `p-cdh`.derivativeactive.dauction;"
]




for query in drop_table_queries + deep_clone_queries:
    print(query)
    spark.sql(query)

# COMMAND ----------

# MAGIC %sql
# MAGIC select max(file_date) from `d-cdh`.landed.newsphone_auctions

# COMMAND ----------

# MAGIC %sql
# MAGIC select max(file_date) from `p-cdh`.landing.newsphone_auctions

# COMMAND ----------

# DBTITLE 1,deep clone and truncate
drop_table_queries = [
    "DROP TABLE IF EXISTS `d-cdh`.landing.newsphone_auctions;",
    "DROP TABLE IF EXISTS `d-cdh`.landed.newsphone_auctions;",
    "DROP TABLE IF EXISTS `d-cdh`.landing.newsphone_basic_data;",
    "DROP TABLE IF EXISTS `d-cdh`.landed.newsphone_basic_data;"
]

deep_clone_queries = [
    "CREATE OR REPLACE TABLE `d-cdh`.landing.newsphone_basic_data DEEP CLONE `p-cdh`.landing.newsphone_basic_data;",
    "CREATE OR REPLACE TABLE `d-cdh`.landed.newsphone_basic_data DEEP CLONE `p-cdh`.landed.newsphone_basic_data;",
    "CREATE OR REPLACE TABLE `d-cdh`.landing.newsphone_auctions DEEP CLONE `p-cdh`.landing.newsphone_auctions;",
    "CREATE OR REPLACE TABLE `d-cdh`.landed.newsphone_auctions DEEP CLONE `p-cdh`.landed.newsphone_auctions;"
]

truncate_table_queries = [
    "TRUNCATE TABLE `d-cdh`.landing.newsphone_basic_data;",  
    "TRUNCATE TABLE `d-cdh`.landed.newsphone_basic_data;",
    "TRUNCATE TABLE `d-cdh`.landing.newsphone_auctions;",
    "TRUNCATE TABLE `d-cdh`.landed.newsphone_auctions;"
]

for query in drop_table_queries + deep_clone_queries + truncate_table_queries:
    print(query)
    spark.sql(query)

# COMMAND ----------

# DBTITLE 1,Delete and Deep clone
drop_table_queries = [
    "DROP TABLE IF EXISTS `d-cdh`.primaryactive.auctionrealestate;", 
    "DROP TABLE IF EXISTS `d-cdh`.derivativeactive.dauctionrealestate;",
    "DROP TABLE IF EXISTS `d-cdh`.primaryactive.auction;", 
    "DROP TABLE IF EXISTS `d-cdh`.derivativeactive.dauction;"
]

deep_clone_queries = [
    "CREATE OR REPLACE TABLE `d-cdh`.primaryactive.auctionrealestate DEEP CLONE `p-cdh`.primaryactive.auctionrealestate;", 
    "CREATE OR REPLACE TABLE `d-cdh`.derivativeactive.dauctionrealestate DEEP CLONE `p-cdh`.derivativeactive.dauctionrealestate;",
    "CREATE OR REPLACE TABLE `d-cdh`.primaryactive.auction DEEP CLONE `p-cdh`.primaryactive.auction;", 
    "CREATE OR REPLACE TABLE `d-cdh`.derivativeactive.dauction DEEP CLONE `p-cdh`.derivativeactive.dauction;"
]

delete_queries = [
    f""" delete from `d-cdh`.primaryactive.auctionrealestate where SourceSystem = 'extfile';""",
    f""" delete from `d-cdh`.primaryactive.auction where SourceSystem = 'extfile';""",
    f""" delete from `d-cdh`.derivativeactive.dauction where SourceSystem = 'extfile';""", 
    f""" delete from `d-cdh`.derivativeactive.dauctionrealestate where SourceSystem = 'extfile';"""
    ]


for query in drop_table_queries + deep_clone_queries + delete_queries:
    print(query)
    spark.sql(query)

# COMMAND ----------

tables = [
    "`d-cdh`.landing.newsphone_auctions",
    "`d-cdh`.landed.newsphone_auctions",
    "`d-cdh`.landing.newsphone_basic_data",
    "`d-cdh`.landed.newsphone_basic_data"
]

for table in tables:
    query = f"SELECT * FROM {table}"
    df = spark.sql(query)
    display(df)

# COMMAND ----------

tables =   [  "`d-cdh`.primaryactive.auctionrealestate",
    "`d-cdh`.derivativeactive.dauctionrealestate",
    "`d-cdh`.primaryactive.auction",
    "`d-cdh`.derivativeactive.dauction" ]


for table in tables:
    query = f"SELECT * FROM {table} LIMIT 100"
    df = spark.sql(query)
    display(df)


# COMMAND ----------

# Define your environment prefixes
env_prefix = "p"       # or your source environment
dev_env_prefix = "d"    # or your destination environment
 
# List of tables
tables = [
    "landed.thesis_prproperty",
    "landed.thesis_prproperty_building",
    "landed.thesis_prlanduse",
    "landed.thesis_prpropertytemp",
    "landed.thesis_prpropertybuildingtemp",
    "landed.qca_fsad_real_estate_investigations_properties",
    "landed.qca_fs_real_estate_investigations",
    "landed.qca_fsad_properties_real_estates",
    "landed.arotron_eaiSIG_RE",
    "landed.arotron_eaiSIG_REPE"
]
 
# Generate clone queries
clone_queries = [
    f"""
    CREATE OR REPLACE TABLE `{dev_env_prefix}-cdh`.{table} AS
    SELECT *
    FROM `{env_prefix}-cdh`.{table}
    WHERE BusinessDate = (
        SELECT MAX(BusinessDate) FROM `{env_prefix}-cdh`.{table}
    );
    """
    for table in tables
]
# Execute and print each query
for query in clone_queries:
    print(f"Running query:\n{query}")
    spark.sql(query)

# COMMAND ----------

tables = [
    "landed.thesis_prproperty",
    "landed.thesis_prproperty_building",
    "landed.thesis_prlanduse",
    "landed.thesis_prpropertytemp",
    "landed.thesis_prpropertybuildingtemp",
    "landed.qca_fsad_real_estate_investigations_properties",
    "landed.qca_fs_real_estate_investigations",
    "landed.qca_fsad_properties_real_estates",
    "landed.arotron_eaiSIG_RE",
    "landed.arotron_eaiSIG_REPE"
]

max_business_dates = {}
for table in tables:
    query = f"SELECT MAX(BusinessDate) as max_date FROM `p-cdh`.{table}"
    df = spark.sql(query)
    max_business_dates[table] = df.collect()[0]['max_date']

max_business_dates

# COMMAND ----------

# Define your environment prefixes
env_prefix = "p"       # or your source environment
dev_env_prefix = "d"    # or your destination environment
 
# List of tables
tables = [
    "landed.thesis_prproperty",
    "landed.thesis_prproperty_building",
    "landed.thesis_prlanduse",
    "landed.thesis_prpropertytemp",
    "landed.thesis_prpropertybuildingtemp",
    "landed.qca_fsad_real_estate_investigations_properties",
    "landed.qca_fs_real_estate_investigations",
    "landed.qca_fsad_properties_real_estates",
    "landed.arotron_eaiSIG_RE",
    "landed.arotron_eaiSIG_REPE"
]
 
# Generate clone queries
clone_queries = [
    f"""
    CREATE OR REPLACE TABLE `{dev_env_prefix}-cdh`.{table} AS
    DEEP CLONE `{env_prefix}-cdh`.{table}
    """
    for table in tables
]
# Execute and print each query
for query in clone_queries:
    print(f"Running query:\n{query}")
    spark.sql(query)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE `d-cdh`.primaryactive.realestatebuilding

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `d-cdh`.primaryactive.realestatebuilding
# MAGIC DEEP CLONE `p-cdh`.primaryactive.realestatebuilding
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC describe his from `d-cdh`.primaryactive.realestatebuilding

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) from `p-cdh`.primaryactive.realestatebuilding

# COMMAND ----------


