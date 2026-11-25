# Databricks notebook source
# MAGIC %md
# MAGIC ### Portfolio updates
# MAGIC ## Steps 
# MAGIC * Run the following sql queries
# MAGIC * Upload the new portfolio.json in prod enviroment

# COMMAND ----------

spark.sql('''
UPDATE `p-cdh`.primaryactive.portfolio
SET Short_Name_Asset_Owner = 'Hoist'
WHERE PortfolioName in ('Cell','Light','Mercury','Orbit','Pearl Portfolio A')
''')


# COMMAND ----------

spark.sql('''
UPDATE `p-cdh`.primaryactive.portfolio 
SET Short_Name_Asset_Owner = 'DK'
WHERE PortfolioName in ('Cosmos Retail','Cosmos Wholesale','Hermes Portfolio B','Orion X Securitisation DAC','Galaxy II Funding')
''')

# COMMAND ----------

# MAGIC %md
# MAGIC ### CAMPT-124

# COMMAND ----------


spark.sql(f"""
ALTER TABLE `p-cdh`.derivativeactive.dportfoliotoolcore
ADD COLUMN AttributionCOA string AFTER PastAuctionResult;
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DBTC-595

# COMMAND ----------

env = "p"

spark.sql(f'''alter table `{env}-cdh`.primaryactive.party
add column SubServicer string after TargetLegalAction ;''')

# COMMAND ----------

spark.sql(f'''alter table `{env}-cdh`.primaryarchive.party
add column SubServicer string after TargetLegalAction ;''')

# COMMAND ----------

spark.sql(f'''
MERGE INTO `p-cdh`.primaryactive.party AS target
USING(
  SELECT party.PartyKey,
         party.BusinessDate,
         l1.LOV_DESC
  FROM `{env}-cdh`.primary.party 
  JOIN `{env}-cdh`.landed.qc_customer qccust 
      ON qccust.cust_code = party.PartyKey 
      AND qccust.BusinessDate = party.BusinessDate
  JOIN `{env}-cdh`.landed.qc_lst_of_val AS l1
      ON qccust.SpecialCases = l1.LOV_CODE
      and qccust.BusinessDate = l1.BusinessDate 
  WHERE party.sourcesystem = 'qc' -- and party.BusinessDate = '2024-12-02'
  and l1.LOVT_CODE = 764 
  and l1.LOV_DESC = 'EUCLID DV01 - ASSIGN FOR SERVICING'
) AS source
ON target.PartyKey = source.PartyKey
AND target.BusinessDate = source.BusinessDate
AND target.sourcesystem = 'qc'
WHEN MATCHED THEN UPDATE SET target.SubServicer = source.LOV_DESC
''')

# COMMAND ----------

spark.sql(f'''alter table `{env}-cdh`.derivativearchive.dparty
add column SubServicer string after TargetLegalAction ;''')

# COMMAND ----------

spark.sql(f'''alter table `{env}-cdh`.derivativeactive.dparty
add column SubServicer string after TargetLegalAction ;''')

# COMMAND ----------

spark.sql(f"""
MERGE INTO `{env}-cdh`.derivativeactive.dparty AS target
USING(
  SELECT party.CDI,
         party.BusinessDate,
         party.SubServicer as SubServicer_new,
         party.sourcesystem
  FROM `{env}-cdh`.primary.party 
  WHERE party.sourcesystem = 'qc' -- and party.BusinessDate = '2024-12-02'
  and party.SubServicer = 'EUCLID DV01 - ASSIGN FOR SERVICING'
) AS source
ON target.CDI = source.CDI
AND target.BusinessDate = source.BusinessDate
WHEN MATCHED THEN UPDATE SET target.SubServicer = source.SubServicer_new
""")

# COMMAND ----------

spark.sql(f'''alter table `{env}-cdh`.derivativeactive.dexposure
add column SubServicer string after CourtRegion;''')

# COMMAND ----------

spark.sql(f'''alter table `{env}-cdh`.derivativearchive.dexposure
add column SubServicer string after CourtRegion;''')

# COMMAND ----------

spark.sql(f'''
MERGE INTO `{env}-cdh`.derivativeactive.dexposure AS dexposure
USING `{env}-cdh`.primary.party
ON dexposure.CDI = party.CDI 
   AND dexposure.BusinessDate = party.BusinessDate
   AND party.SubServicer = 'EUCLID DV01 - ASSIGN FOR SERVICING'
WHEN MATCHED THEN
    UPDATE SET dexposure.SubServicer = party.SubServicer;
''')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Refresh simple views
# MAGIC https://dbc-7a36af91-f82b.cloud.databricks.com/editor/notebooks/610302587438997?o=6854132182247269#command/610302587441651

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create reference tables 
# MAGIC https://dbc-7a36af91-f82b.cloud.databricks.com/editor/notebooks/1034407520718559?o=6854132182247269#command/1034407520718560

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add the corresponding comments in multiple columns of CUP tables
# MAGIC
# MAGIC https://dbc-7a36af91-f82b.cloud.databricks.com/editor/notebooks/1435086249829026?o=6854132182247269#command/1435086249829027
