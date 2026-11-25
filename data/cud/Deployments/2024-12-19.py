# Databricks notebook source
# MAGIC %md
# MAGIC ## DBTC-845

# COMMAND ----------

env="p"

# COMMAND ----------

spark.sql("""
ALTER TABLE `{env}-cdh`.derivativeactive.dexposure
ADD COLUMN IFRSGBV decimal(19,2) AFTER SubServicer;
""")

# COMMAND ----------

spark.sql("""
ALTER TABLE `{env}-cdh`.derivativearchive.dexposure
ADD COLUMN IFRSGBV decimal(19,2) AFTER SubServicer;
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## DATA2-80

# COMMAND ----------

spark.sql("""
ALTER TABLE `{env}-cdh`.primaryactive.party
ADD COLUMN OrganizationOriginCountry string AFTER SubServicer;          
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ##RBTC-131 

# COMMAND ----------

spark.sql(f'''alter table `{env}-cdh`.primaryactive.CaseCode
add column AppealCode string after MinimumPaymentAmount''')

# COMMAND ----------

spark.sql(f'''alter table `{env}-cdh`.primaryarchive.CaseCode
add column AppealCode string after MinimumPaymentAmount''')

# COMMAND ----------

spark.sql(f'''alter table `{env}-cdh`.primaryactive.CaseCode
add column ActiveAppealFlag string after AppealCode''')

# COMMAND ----------

spark.sql(f'''alter table `{env}-cdh`.primaryarchive.CaseCode
add column ActiveAppealFlag string after AppealCode''')

# COMMAND ----------

spark.sql(f'''alter table `{env}-cdh`.primaryactive.CaseCode
add column DecisionDate string after ActiveAppealFlag''')

# COMMAND ----------

spark.sql(f'''alter table `{env}-cdh`.primaryarchive.CaseCode
add column DecisionDate string after ActiveAppealFlag''')

# COMMAND ----------

spark.sql(f'''alter table `{env}-cdh`.primaryactive.CaseCode
add column CommitteeDecisionTypeCode int after DecisionDate''')

# COMMAND ----------

spark.sql(f'''alter table `{env}-cdh`.primaryarchive.CaseCode
add column CommitteeDecisionTypeCode int after DecisionDate''')

# COMMAND ----------

spark.sql(f'''alter table `{env}-cdh`.primaryactive.CaseCode
add column CommitteeDecisionType string after CommitteeDecisionTypeCode''')

# COMMAND ----------

spark.sql(f'''alter table `{env}-cdh`.primaryarchive.CaseCode
add column CommitteeDecisionType string after CommitteeDecisionTypeCode''')

# COMMAND ----------

spark.sql(f''' 
MERGE INTO `{env}-cdh`.primaryactive.CaseCode AS CaseCode
USING `{env}-cdh`.landed.qca_fsad_case_coc_appeals AS coc_appeals
ON CaseCode.CaseCodeKey = coc_appeals.CASE_CODE
   AND CaseCode.BusinessDate = coc_appeals.BusinessDate
   AND CaseCode.sourcesystem = 'qca'
WHEN MATCHED THEN
    UPDATE SET 
        CaseCode.AppealCode = coc_appeals.CCAP_CODE,
        CaseCode.ActiveAppealFlag = coc_appeals.CCAP_ACTIVE_FLAG,
        CaseCode.DecisionDate = coc_appeals.CCAP_COMMITTEE_DECISION_DATE,
        CaseCode.CommitteeDecisionTypeCode = 
            CASE
                WHEN coc_appeals.CCAP_COMMITTEE_DECISION_CODE IS NULL THEN NULL
                WHEN TRIM(CAST(coc_appeals.CCAP_COMMITTEE_DECISION_CODE AS STRING)) = '' THEN NULL
                ELSE coc_appeals.CCAP_COMMITTEE_DECISION_CODE
            END,
        CaseCode.CommitteeDecisionType = 
            CASE
                WHEN coc_appeals.CCAP_COMMITTEE_DECISION_CODE = 14062 THEN 'N/A'
                WHEN coc_appeals.CCAP_COMMITTEE_DECISION_CODE = 14063 THEN 'Θετική'
                WHEN coc_appeals.CCAP_COMMITTEE_DECISION_CODE = 14064 THEN 'Απορριπτική'
                ELSE NULL
            END;

''')

# COMMAND ----------



spark.sql(f'''
MERGE INTO `{env}-cdh`.primaryactive.CaseCode 
USING `{env}-cdh`.landed.qc_case_master qc_master
ON CaseCode.CaseCodeKey = qc_master.case_code
   AND CaseCode.BusinessDate = qc_master.BusinessDate
   AND CaseCode.sourcesystem = 'qc'
WHEN MATCHED THEN
    UPDATE SET 
        CaseCode.AppealCode = -1,
        CaseCode.ActiveAppealFlag = NULL,
        CaseCode.DecisionDate = CASE 
                                    WHEN qc_master.CEXT_GENERIC_DATETIME_11 IS NULL 
                                    THEN CAST('1900-01-01' AS DATE) 
                                    ELSE CAST(qc_master.CEXT_GENERIC_DATETIME_11 AS DATE) 
                                END,
        CaseCode.CommitteeDecisionTypeCode = NULL,
        CaseCode.CommitteeDecisionType = NULL;
''')

# COMMAND ----------

spark.sql(f'''
MERGE INTO `{env}-cdh`.primaryactive.Exposure ex
USING `{env}-cdh`.landed.profits_params p_params
ON ex.RestructuringPEETypeCode = p_params.serial_num
AND ex.businessdate = p_params.businessdate
AND p_params.param_type = 'ADJ02'
AND ex.sourcesystem="profits"
WHEN MATCHED THEN 
    UPDATE SET
       ex.RestructuringPEEType = p_params.DESCRIPTION
''')

# COMMAND ----------

create_reference_table =f"""
CREATE TABLE `p-cdh`.reference.monzadatatapeclosing
AS SELECT * FROM `qa-cdh`.reference.monzadatatapeclosing;"""
spark.sql(create_reference_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Refresh Views !!

# COMMAND ----------

views_df = spark.sql("""
    SELECT table_name, table_schema, table_catalog
    FROM system.information_schema.tables
    WHERE table_catalog in ('{env}-cdh')
      AND table_schema in ('derivative', 'primary')
      and table_type='VIEW'
""")

# display(views_df)

for view_row in views_df.collect():
    table_name = view_row["table_name"]
    table_schema = view_row["table_schema"]
    table_catalog = view_row["table_catalog"]

    # print(table_name)

    full_view_name =f"`{table_catalog}`.{table_schema}.{table_name}"

    describe_full_view_name = spark.sql(f"""
              DESCRIBE EXTENDED {full_view_name}""")
    
    describe_full_view_name.createOrReplaceTempView("describe_full_view_name")

   # print(describe_full_view_name)

    describe_query = spark.sql(f"""
              select data_type--REGEXP_REPLACE(TRIM(data_type), '[^a-zA-Z0-9*<> ]', '') 
              AS cleaned_view_text 
              from describe_full_view_name 
              where col_name ='View Text'
              """)
    
    sql_view_creation = describe_query.collect()[0][0]

    create_or_replace_sql = f"""
          create or replace view {full_view_name} as 
          {sql_view_creation}
          """
    
    spark.sql(create_or_replace_sql)


