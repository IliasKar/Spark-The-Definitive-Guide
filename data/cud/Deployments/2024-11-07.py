# Databricks notebook source
# MAGIC %md
# MAGIC ## D360-2097 
# MAGIC Addition of multiple fields in portfolio table and also update of already existing ones.
# MAGIC
# MAGIC ### Steps 
# MAGIC * Merge the current pull request.
# MAGIC * Add the new portfolio.json in the corresponding path of common-zone in each enviroment.
# MAGIC * Run the following sql statements
# MAGIC * Run adhoc portfolio for one businessdate ( preferably 2024-11-07) 
# MAGIC * Populate the p-cdh.primary.portfolio with data from hive-metastore
# MAGIC
# MAGIC

# COMMAND ----------


import os 
env = os.environ["S3PREFIX"]  ## or use env="p" instead

spark.sql(f'''alter table `{env}-cdh`.primaryactive.portfolio
add column IsBankingFlag boolean after IsSingleTicketFlag;''')

spark.sql('''alter table `{env}-cdh`.primaryactive.portfolio
add column IsSecuritizedFlag boolean after IsBankingFlag;''')

spark.sql(f'''alter table `{env}-cdh`.primaryactive.portfolio
add column IsHAPSFlag boolean after IsSecuritizedFlag;''')

spark.sql(f'''alter table `{env}-cdh`.primaryactive.portfolio
add column isREoCOFlag boolean after IsHAPSFlag;''')

spark.sql(f'''alter table `{env}-cdh`.primaryactive.portfolio
add column isCostPlusFlag boolean after isREoCOFlag;''')

spark.sql(f'''alter table `{env}-cdh`.primaryactive.portfolio
add column RetailWholesale string after isREoCOFlag;''')

spark.sql(f'''alter table `{env}-cdh`.primaryactive.portfolio
add column closingdate date after RetailWholesale;''')

spark.sql(f'''alter table `{env}-cdh`.primaryactive.portfolio
add column Type_of_Portfolio string after closingdate;''')

spark.sql('''alter table `{env}-cdh`.primaryactive.portfolio
add column Short_Name_Asset_Owner string after Type_of_Portfolio;''')

spark.sql(f'''alter table `{env}-cdh`.primaryactive.portfolio
add column Short_Name_Portfolio_Name string after Short_Name_Asset_Owner;''')

spark.sql(f'''alter table `{env}-cdh`.primaryactive.portfolio
add column OwnerId int after Short_Name_Portfolio_Name;''')


# COMMAND ----------

# MAGIC %md
# MAGIC ## DATA2-49_c
# MAGIC ### Add the correct files for External Files Mechanism
# MAGIC
# MAGIC ### Steps:
# MAGIC * Merge the current PR
# MAGIC * **CHANGE** the parameter "file_name" to "relative_root_path" in **both** Incremental and Snapshot Jobs in **PROD**
# MAGIC * Run the following code in a cell in a notebook in **PROD** environment:

# COMMAND ----------

# Create the ExternalFilesConfiguration table
spark.sql("""
CREATE TABLE IF NOT EXISTS `p-cdh`.`workflow`.ExternalFilesConfiguration (
    RelativePath STRING,
    UnityCatalog STRING,
    UnitySchema STRING,
    TableName STRING,
    Flow STRING,
    ValidatorScript STRING,
    NotificationEmails STRING
)
USING delta;
""")

# Add Constraint for Flow column
spark.sql("""
ALTER TABLE `p-cdh`.`workflow`.ExternalFilesConfiguration ADD CONSTRAINT FlowConstraint CHECK (LOWER(Flow) IN ('incremental', 'snapshot'));
""")

# Insert values
spark.sql("""
INSERT INTO `p-cdh`.`workflow`.ExternalFilesConfiguration
(RelativePath, UnityCatalog, UnitySchema, TableName, Flow, ValidatorScript, NotificationEmails)
VALUES ('EDA', 'p-eda', 'perimeters', NULL, 'Snapshot', NULL, NULL);
""")

spark.sql("""
INSERT INTO `p-cdh`.`workflow`.ExternalFilesConfiguration
(RelativePath, UnityCatalog, UnitySchema, TableName, Flow, ValidatorScript, NotificationEmails)
VALUES ('EDA', 'p-eda', 'perimeters', NULL, 'Incremental', NULL, NULL);
""")

spark.sql("""
INSERT INTO `p-cdh`.`workflow`.ExternalFilesConfiguration
(RelativePath, UnityCatalog, UnitySchema, TableName, Flow, ValidatorScript, NotificationEmails)
VALUES ('DSE_REFERENCE', 'p-cdh', 'reference', NULL, 'Incremental', NULL, NULL);
""")

spark.sql("""
INSERT INTO `p-cdh`.`workflow`.ExternalFilesConfiguration
(RelativePath, UnityCatalog, UnitySchema, TableName, Flow, ValidatorScript, NotificationEmails)
VALUES ('DSE_REFERENCE', 'p-cdh', 'reference', NULL, 'Snapshot', NULL, NULL);
""")
