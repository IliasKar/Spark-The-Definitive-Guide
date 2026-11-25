# Databricks notebook source
import logging
import boto3

from pyspark.sql.functions import coalesce, concat_ws, substring, when, col, regexp_replace, trim
from pyspark.sql import DataFrame

import delta_table_creator, extract, transform
from delta import DeltaTable  # type: ignore

# Import the module
import sys
sys.path.append("../aws_utils")

import aws_utils
# Get Environment Prefix
env = aws_utils.get_env_prefix()

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Create widgets with default values. Change them if needed from UI
# MAGIC * run_date : '2024-01-31'
# MAGIC * unity : True | False
# MAGIC
# MAGIC In case of Unity Catalog, the appropriate cluster should be attached

# COMMAND ----------

# Takes the values of widgets
run_date = dbutils.widgets.get("run_date") 
# table_name = dbutils.widgets.get("table_name") 
table_name = 'DPortfolioToolThirdParties'
unity = dbutils.widgets.get("unity") 

# """
# Takes the s3 parameter value from ssm

# ssm_client = boto3.client("ssm", region_name="eu-central-1") # Initialize the SSM client

# response = ssm_client.get_parameter(
#     Name='/cfn/a360/matching/conf_s3_path',
#     WithDecryption=False
# )

# s3_path = response['Parameter']['Value']
# """

# COMMAND ----------

schema = (
"ExposureKey string,"
"PortfolioName string,"
"WorkableFlag boolean,"
# "WorkableFlagMonthly boolean,"
"NonWorkableReason string,"
"FrequencyType string,"
"DebtNumber string,"
# "NonWorkableReasonMonthly string,"
# "LastMonthMinFinalBucket string,"
"BusinessDate date"
)

# COMMAND ----------

partition_columns = ["BusinessDate"]
derivative_table_path = f"`{env}-cdh`.derivativeactive.{table_name}"
 
# ================== Create Delta Table ==================
logging.info("Creating Delta Table...")
if spark.catalog.tableExists(derivative_table_path) is False:
    create_delta_table(
        spark,
        derivative_table_path,
        partition_columns,
        schema,  # type: ignore
    )
else:
    logging.info("Table already exists.")
 
 
# ================== Delete From Delta Table ==================
 
transform.delete_from_unity_table(
    spark,
    derivative_table_path,
    run_date,  # type: ignore
)

# COMMAND ----------

LoanWorkable = spark.sql(f"""
WITH NonWorkableReasonCalculated AS (
  SELECT 
     DE.ExposureKey, 
     DE.Bucket,
     DE.BusinessDate,
     Queue, 
     AssignmentProcess, 
     Segment, 
     StrategyStep, 
     DE.PortfolioName,
     cm.case_contract as DebtNumber,
     CASE 
          -- Light
          WHEN DE.PortfolioName = 'Light' THEN 
               CASE 
                    WHEN DE.AllBorrowersDeceasedFlag = True THEN '01. DECEASED'
                    WHEN OutstandingTotalBalance = 0 THEN '02. BALANCE 0'
                    WHEN UPPER(AssignmentProcess) IS NULL AND UPPER(Segment) IS NULL THEN '03. NO SEGMENT'
                    -- Apply for all cases where the result is '04. PASSIVE MANAGEMENT'
                    WHEN 
                        UPPER(AssignmentProcess) LIKE 'PASSIVE%' 
                        OR UPPER(Queue) LIKE '%NO%TREATMENT%' 
                        OR UPPER(Segment) LIKE '%PENDING%' 
                        OR UPPER(Segment) LIKE '%REJEC%' 
                        OR UPPER(Segment) LIKE '%ACCEPTED%' 
                        OR UPPER(Segment) LIKE '%L4605%' 
                    THEN
                        CASE 
                            WHEN UPPER(Segment) LIKE 'L3869%' THEN '04. PASSIVE - L3869'
                            WHEN UPPER(AssignmentProcess) LIKE '%NO_SEGMENT%' THEN '04. PASSIVE - NO SEGMENT'
                            WHEN UPPER(Segment) LIKE '%SECURED 1ST LIEN%' THEN '04. PASSIVE - SECURED 1ST LIEN'
                            WHEN UPPER(Segment) LIKE 'L4605%' THEN '04. PASSIVE - L4605'
                            WHEN UPPER(AssignmentProcess) LIKE '%SLAVE%' THEN '16. SLAVES'
                            WHEN UPPER(AssignmentProcess) LIKE '%LOW DEBT%' THEN '14. LOW DEBT'
                            WHEN UPPER(AssignmentProcess) LIKE '%B0%' THEN '09. BUCKET 0'
                            WHEN UPPER(AssignmentProcess) LIKE '%GEFYRA%' THEN '05. EXCLUSIONS'
                            WHEN UPPER(AssignmentProcess) = 'PASSIVE' AND UPPER(Queue) LIKE '%DEFAULT%' THEN '10. CHECK CASES'
                            ELSE '04. PASSIVE MANAGEMENT'
                        END
                    WHEN UPPER(AssignmentProcess) LIKE 'EXC%' 
                         OR UPPER(Queue) IN ('N.3869_ΈΝΑΡΞΗ_ΔΙΑΔΙΚΑΣΊΑΣ_ΈΚΠΤΩΣΗΣ', 'ΠΛΗΓΈΝΤΕΣ', 'ΑΝΑΣΤΟΛΉ ΌΧΛΗΣΗΣ BY ASSET OWNER', 'ΑΝΑΣΤΟΛΉ ΌΧΛΗΣΗΣ ALL PORTFOLIOS', 'GEFYRA ACCOUNT DIMOSIOU')
                         OR UPPER(Queue) LIKE 'OCW%' THEN '05. EXCLUSIONS'
                    WHEN UPPER(AssignmentProcess) LIKE 'FULF%' 
                         OR UPPER(Queue) IN ('LIGHT FULFILLED SETTLEMENTS', 'LIGHT FULFILLED SETTLEMENTS TO RECALL', 'N.3869_ΕΞΟΦΛΉΣΕΙΣ', 'PAID OFF PRIOR MIGRATION')
                         OR UPPER(Queue) LIKE '%FULFILLED%'
                         OR UPPER(Queue) LIKE '%ALMOST PAID OFF%'
                         OR UPPER(StrategyStep) = 'NOP FULFILLED SETT' THEN '06. FULFILLED SETTLEMENT / PAID OFF'
                    WHEN UPPER(AssignmentProcess) LIKE 'UNH%' AND 
                         (UPPER(Queue) NOT LIKE '%OFFER%' AND UPPER(Queue) NOT LIKE '%COLLECTION%') THEN '07. TO BE ASSIGNED'
                    WHEN UPPER(Queue) LIKE '%TO%ASS%' 
                         OR UPPER(StrategyStep) = 'NOP NON ASSIGN CASES' THEN '08. TO BE ASSIGNED'
                    WHEN UPPER(Queue) LIKE '%BUCKET00%' 
                         OR UPPER(Queue) LIKE '%B00%' 
                         OR DE.Bucket = 0 
                         OR UPPER(StrategyStep) = 'NOP CURRENT BUCKET 0' 
                         OR UPPER(StrategyStep) LIKE 'NOP SETTLEMENT CB00%' THEN '09. BUCKET 0'
                    WHEN UPPER(Queue) LIKE '%DEFAULT%' 
                         OR UPPER(StrategyStep) LIKE '%NOP%CHECK%' THEN '10. CHECK CASES'
                    WHEN UPPER(Queue) LIKE '%ΕΞΌΦΛΗΣΗ%' THEN '11. CLAIM TO HAVE PAID OFF'
                    WHEN UPPER(Queue) = 'N3869_ΕΞΑΊΡΕΣΗ_ΔΙΑΧΕΊΡΙΣΗΣ_ΠΑΡΆΠΟΝΑ'
                         OR UPPER(StrategyStep) = 'COMPLAINT' THEN '12. COMPLAINT'
                    WHEN UPPER(Queue) IN ('MRC_CURRENT BUCKET 01 L3758', 'ORBIT_CURRENT BUCKET 01 L3758', 'LIGHT_CURRENT BUCKET 01 L3758', 'LIGHT_SETTLEMENT BUCKET 01 L3758_NON ASSIG') 
                         OR UPPER(StrategyStep) = 'NOP L3758' THEN '13. DPD <10'
                    WHEN UPPER(Queue) IN ('LIGHT_GRACE <10€ EXT', 'LIGHT_GRACE <10€ EXT_JUDICIAL', 'LIGHT CB1+ <30€ EXT', 'LIGHT CB1+ <30€ EXT_JUDICIAL', 'LIGHT CB1+ <30€ EXT_L3869', 'LIGHT CB1+ <30€_JUDICIAL', 'LIGHT CB2-3 <30€_JUDICIAL', 'LIGHT CB1+ <30€_JUDICIAL', 'LIGHT_GRACE <10€_JUDICIAL') 
                         OR UPPER(StrategyStep) = 'NOP GRACE <50€' THEN '14. LOW DEBT'
                    WHEN UPPER(Queue) = 'TEST' THEN '15. NOT ASSIGNED'
                    WHEN UPPER(Queue) LIKE '%WDN%' 
                         OR UPPER(Queue) LIKE '%ORPHAN%' 
                         OR UPPER(Queue) LIKE '%SLAVE%' THEN '16. SLAVES'
                    WHEN UPPER(StrategyStep) = 'ZERO PHONES' THEN '17. ZERO PHONES'
                    WHEN UPPER(StrategyStep) LIKE 'NOP PEND%' 
                         OR UPPER(StrategyStep) = 'NOP AWAITING' THEN '18. PENDING DOCUMENTS OR APPOINTMENT'
                    WHEN UPPER(StrategyStep) LIKE 'NOP % WITH PROMISE' THEN '19. CUSTOMER WITH PROMISE'
                    WHEN UPPER(StrategyStep) LIKE 'NOP%RPC%' 
                         OR UPPER(StrategyStep) = 'NOP HOLD 15D' THEN '21. ON HOLD - RPC'
                    WHEN UPPER(Queue) IN ('L3869 POS COMMEN DAY', 'L3869 POS NO PAYMENT') THEN '22. FINAL L3869 NO CURRENT/FUTURE PAYMENTS'
                    ELSE '21. WORKABLE'
               END

          -- Orbit
          WHEN DE.PortfolioName = 'Orbit' THEN 
               CASE 
                   WHEN DE.AllBorrowersDeceasedFlag = True THEN '01. DECEASED'
                   WHEN OutstandingTotalBalance = 0 THEN '02. BALANCE 0'
                   WHEN UPPER(AssignmentProcess) IS NULL AND UPPER(Segment) IS NULL THEN '03. NO SEGMENT'
                   -- Apply for all cases where the result is '04. PASSIVE MANAGEMENT'
                   WHEN 
                        UPPER(AssignmentProcess) LIKE 'PASSIVE%' 
                        OR UPPER(Queue) LIKE '%NO%TREATMENT%' 
                        OR UPPER(Segment) LIKE '%PENDING%' 
                        OR UPPER(Segment) LIKE '%REJEC%' 
                        OR (UPPER(Segment) LIKE 'L3869 ACCEPTED%' AND DE.Bucket > 3) 
                        OR UPPER(Segment) LIKE '%L4605%' 
                   THEN
                        CASE 
                            WHEN UPPER(Segment) LIKE 'L3869%' THEN '04. PASSIVE - L3869'
                            WHEN UPPER(AssignmentProcess) LIKE '%NO_SEGMENT%' THEN '04. PASSIVE - NO SEGMENT'
                            WHEN UPPER(Segment) LIKE '%SECURED 1ST LIEN%' THEN '04. PASSIVE - SECURED 1ST LIEN'
                            WHEN UPPER(Segment) LIKE 'L4605%' THEN '04. PASSIVE - L4605'
                            WHEN UPPER(AssignmentProcess) LIKE '%SLAVE%' THEN '16. SLAVES'
                            WHEN UPPER(AssignmentProcess) LIKE '%LOW DEBT%' THEN '14. LOW DEBT'
                            WHEN UPPER(AssignmentProcess) LIKE '%B0%' THEN '09. BUCKET 0'
                            WHEN UPPER(AssignmentProcess) LIKE '%GEFYRA%' THEN '05. EXCLUSIONS'
                            WHEN UPPER(AssignmentProcess) = 'PASSIVE' AND UPPER(Queue) LIKE '%DEFAULT%' THEN '10. CHECK CASES'
                            ELSE '04. PASSIVE MANAGEMENT'
                        END
                   WHEN UPPER(AssignmentProcess) LIKE 'EXC%' THEN '05. EXCLUSIONS'
                   WHEN UPPER(Queue) = 'GEFYRA ACCOUNT DIMOSIOU' THEN '05. EXCLUSIONS'
                   WHEN UPPER(Queue) IN ('N.3869_ΈΝΑΡΞΗ_ΔΙΑΔΙΚΑΣΊΑΣ_ΈΚΠΤΩΣΗΣ', 'ΠΛΗΓΈΝΤΕΣ', 'ΑΝΑΣΤΟΛΉ ΌΧΛΗΣΗΣ BY ASSET OWNER', 'ΑΝΑΣΤΟΛΉ ΌΧΛΗΣΗΣ ALL PORTFOLIOS')
                        OR UPPER(Queue) LIKE 'OCW%' THEN '05. EXCLUSIONS'
                   WHEN UPPER(AssignmentProcess) LIKE 'FULF%' THEN '06. FULFILLED SETTLEMENT / PAID OFF'
                   WHEN UPPER(Queue) IN ('MERCURY ALMOST PAID OFF', 'PAID OFF PRIOR MIGRATION', 'N.3869_ΕΞΟΦΛΉΣΕΙΣ') 
                        OR UPPER(Queue) LIKE '%FULFILLED%' THEN '06. FULFILLED SETTLEMENT / PAID OFF'
                   WHEN UPPER(StrategyStep) = 'NOP FULFILLED SETT' THEN '06. FULFILLED SETTLEMENT / PAID OFF'
                   WHEN UPPER(AssignmentProcess) LIKE 'UNH%' AND 
                        UPPER(Queue) NOT IN ('ORB OFFER CA', 'ORB OFFER VIP CA', 'ORBIT COLLECTION SETTLEMENT ASSIGN TO INT', 'ORB COLLECTION TO BE ASSIGN') 
                        THEN '07. TO BE ASSIGNED'
                   WHEN UPPER(Queue) LIKE '%TOBE ASS3869%' 
                        OR UPPER(Queue) LIKE 'ORBIT TO BE ASSIGN%' THEN '08. TO BE ASSIGNED'
                   WHEN UPPER(Queue) = 'MRC_CURRENT_BUCKET00' 
                        OR UPPER(Queue) = 'ORBIT_CURRENT_BUCKET00' THEN '09. BUCKET 0'
                   WHEN DE.Bucket = 0 THEN '09. BUCKET 0'
                   WHEN UPPER(StrategyStep) = 'NOP CURRENT BUCKET 0' THEN '09. BUCKET 0'
                   WHEN UPPER(StrategyStep) LIKE 'NOP SETTLEMENT CB00%' THEN '09. BUCKET 0'
                   WHEN UPPER(Queue) LIKE '%DEFAULT' THEN '10. CHECK CASES'
                   WHEN UPPER(Queue) = 'ΙΣΧΥΡΙΣΜΌΣ_ΕΞΌΦΛΗΣΗΣ' 
                        OR UPPER(Queue) = 'MERCURY ΙΣΧΥΡΙΣΜΌΣ ΟΦΕΙΛΈΤΗ ΕΞΌΦΛΗΣΗΣ' THEN '11. CLAIM TO HAVE PAID OFF'
                   WHEN UPPER(Queue) = 'N3869_ΕΞΑΊΡΕΣΗ_ΔΙΑΧΕΊΡΙΣΗΣ_ΠΑΡΆΠΟΝΑ' THEN '12. COMPLAINT'
                   WHEN UPPER(StrategyStep) = 'COMPLAINT' THEN '12. COMPLAINT'
                   WHEN UPPER(Queue) = 'ORBIT_CURRENT BUCKET 01 L3758' THEN '13. DPD <10'
                   WHEN UPPER(StrategyStep) = 'NOP L3758' THEN '13. DPD <10'
                   WHEN UPPER(Queue) IN ('MRC_GRACE <50€', 'ORBIT CB1+ <30€ EXT', 'ORBIT CB2-3 <30€', 'ORB_GRACE <50€ EXT', 'ORB_GRACE <50€', 'ORBIT_GRACE <50€', 'ORBIT CB1+ <30€') 
                        THEN '14. LOW DEBT'
                   WHEN UPPER(StrategyStep) = 'NOP GRACE <50€' THEN '14. LOW DEBT'
                   WHEN UPPER(Queue) = 'TEST' THEN '15. NOT ASSIGNED'
                   WHEN UPPER(Queue) IN ('ORB ORHANS_NO_PLEDGE', 'ORBIT WDN') THEN '16. SLAVES'
                   WHEN UPPER(Queue) LIKE '%SLAVE%' THEN '16. SLAVES'
                   WHEN UPPER(StrategyStep) = 'ZERO PHONES' THEN '17. ZERO PHONES'
                   WHEN UPPER(StrategyStep) LIKE 'NOP PEND%' THEN '18. PENDING DOCUMENTS OR APPOINTMENT'
                   WHEN UPPER(StrategyStep) LIKE 'NOP % WITH PROMISE' THEN '19. CUSTOMER WITH PROMISE'
                   WHEN UPPER(Queue) IN ('L3869 POS COMMEN DAY', 'L3869 POS NO PAYMENT') THEN '20. FINAL L3869 NO CURRENT/FUTURE PAYMENTS'
                   ELSE '21. WORKABLE'
               END

          -- Mercury
          WHEN DE.PortfolioName = 'Mercury' THEN 
               CASE 
                   WHEN DE.AllBorrowersDeceasedFlag = True THEN '01. DECEASED'
                   WHEN OutstandingTotalBalance = 0 THEN '02. BALANCE 0'
                   WHEN UPPER(AssignmentProcess) IS NULL AND UPPER(PartySegment) IS NULL THEN '03. NOSEGMENT'
                   WHEN UPPER(AssignmentProcess) LIKE '%PASSIVE%' AND UPPER(Segment) LIKE '%INDEX%' THEN '04. PASSIVE L3869 APPROVED NON INDEXED'
                   WHEN UPPER(AssignmentProcess) LIKE '%PASSIVE%' AND UPPER(Segment) LIKE '%APPROVED%' THEN '05. PASSIVE L3869 LEVERAGE A/B'
                   WHEN UPPER(AssignmentProcess) LIKE '%PASSIVE%' AND UPPER(Segment) LIKE '%REST%' THEN '06. PASSIVE L3869 PENDING COURT DECISIONS'
                   WHEN UPPER(AssignmentProcess) LIKE 'EXC%' THEN '07. EXCLUSIONS'
                   WHEN UPPER(Queue) = 'GEFYRA ACCOUNT DIMOSIOU' THEN '07. EXCLUSIONS'
                   WHEN UPPER(Queue) IN ('N.3869_ΈΝΑΡΞΗ_ΔΙΑΔΙΚΑΣΊΑΣ_ΈΚΠΤΩΣΗΣ', 'ΠΛΗΓΈΝΤΕΣ', 'ΑΝΑΣΤΟΛΉ ΌΧΛΗΣΗΣ BY ASSET OWNER', 'ΑΝΑΣΤΟΛΉ ΌΧΛΗΣΗΣ ALL PORTFOLIOS', '') 
                        OR UPPER(Queue) LIKE 'OCW%' 
                        OR UPPER(Queue) LIKE '%NO TREATMENT%' THEN '07. EXCLUSIONS'
                   WHEN UPPER(AssignmentProcess) LIKE 'FULF%' THEN '08. FULFILLED SETTLEMENT / PAID OFF'
                   WHEN UPPER(Queue) IN ('MERCURY ALMOST PAID OFF', 'PAID OFF PRIOR MIGRATION', 'N.3869_ΕΞΟΦΛΉΣΕΙΣ')
                        OR UPPER(Queue) LIKE '%FULFILLED%' THEN '08. FULFILLED SETTLEMENT / PAID OFF'
                   WHEN UPPER(StrategyStep) = 'NOP FULFILLED SETT' THEN '08. FULFILLED SETTLEMENT / PAID OFF'
                   WHEN UPPER(AssignmentProcess) LIKE 'UNH%' THEN '09. TO BE ASSIGNED'
                   WHEN UPPER(Queue) LIKE '%TOBE ASS%' OR UPPER(Queue) LIKE '%TO BE ASSIGN%' THEN '09. TO BE ASSIGNED'
                   WHEN DE.Bucket = 0 THEN '10. BUCKET 0'
                   WHEN UPPER(Queue) = 'MRC_CURRENT_BUCKET00' OR UPPER(Queue) = 'ORBIT_CURRENT_BUCKET00' THEN '10. BUCKET 0'
                   WHEN UPPER(StrategyStep) = 'NOP CURRENT BUCKET 0' THEN '10. BUCKET 0'
                   WHEN UPPER(StrategyStep) LIKE 'NOP SETTLEMENT CB00%' THEN '10. BUCKET 0'
                   WHEN UPPER(Queue) LIKE '%DEFAULT' THEN '11. CHECK CASES'
                   WHEN UPPER(Queue) = 'ΙΣΧΥΡΙΣΜΌΣ_ΕΞΌΦΛΗΣΗΣ' OR UPPER(Queue) = 'MERCURY ΙΣΧΥΡΙΣΜΌΣ ΟΦΕΙΛΈΤΗ ΕΞΌΦΛΗΣΗΣ' THEN '12. CLAIM TO HAVE PAID OFF'
                   WHEN UPPER(Queue) = 'N3869_ΕΞΑΊΡΕΣΗ_ΔΙΑΧΕΊΡΙΣΗΣ_ΠΑΡΆΠΟΝΑ' THEN '13. COMPLAINT'
                   WHEN UPPER(StrategyStep) = 'COMPLAINT' THEN '13. COMPLAINT'
                   WHEN UPPER(Queue) = 'MRC_CURRENT BUCKET 01 L3758' OR UPPER(Queue) = 'ORBIT_CURRENT BUCKET 01 L3758' THEN '14. DPD <10'
                   WHEN UPPER(StrategyStep) = 'NOP L3758' THEN '14. DPD <10'
                   WHEN UPPER(Queue) IN ('MRC_GRACE <50€', 'ORBIT CB1+ <30€ EXT', 'ORBIT CB2-3 <30€', 'ORB_GRACE <50€ EXT', 'ORB_GRACE <50€', 'ORBIT_GRACE <50€', 'ORBIT CB1+ <30€') THEN '15. LOW DEBT'
                   WHEN UPPER(StrategyStep) = 'NOP GRACE <50€' THEN '15. LOW DEBT'
                   WHEN UPPER(Queue) = 'TEST' THEN '16. NOT ASSIGNED'
                   WHEN UPPER(Queue) IN ('ORBIT SLAVES ACCOUNT', 'ORB ORHANS_NO_PLEDGE', 'ORBIT WDN') THEN '17. SLAVES'
                   WHEN UPPER(StrategyStep) = 'ZERO PHONES' THEN '18. ZERO PHONES'
                   WHEN UPPER(StrategyStep) LIKE 'NOP PEND%' THEN '19. PENDING DOCUMENTS OR APPOINTMENT'
                   WHEN UPPER(StrategyStep) LIKE 'NOP % WITH PROMISE' THEN '20. CUSTOMER WITH PROMISE'
                   WHEN UPPER(Queue) IN ('L3869 POS COMMEN DAY', 'L3869 POS NO PAYMENT') THEN '22. FINAL L3869 NO CURRENT/FUTURE PAYMENTS' 
                   ELSE '21. WORKABLE'
               END
          
          -- Cell
	     WHEN DE.PortfolioName = 'Cell' THEN 
	          CASE 
	               WHEN DE.AllBorrowersDeceasedFlag = True THEN '01. DECEASED'
	               WHEN OutstandingTotalBalance = 0 THEN '02. BALANCE 0'
	               WHEN UPPER(AssignmentProcess) IS NULL AND UPPER(Segment) IS NULL THEN '03. NO SEGMENT'
                    WHEN 
                        UPPER(AssignmentProcess) LIKE 'PASSIVE%' 
                        OR UPPER(Queue) LIKE '%NO%TREATMENT%' 
                        OR UPPER(Segment) LIKE '%PENDING%' 
                        OR UPPER(Segment) LIKE '%REJEC%>10K%' 
                        OR UPPER(Segment) LIKE '%ACCEPTED%' 
                        OR UPPER(Segment) LIKE '%L4605%' 
                    THEN
                         CASE 
                            WHEN UPPER(Segment) LIKE 'L3869%' AND UPPER(Segment) NOT LIKE '%L3869 REJECTED <10K%' THEN '04. PASSIVE - L3869'
                            WHEN UPPER(AssignmentProcess) LIKE '%NO_SEGMENT%' THEN '04. PASSIVE - NO SEGMENT'
                            WHEN UPPER(Segment) LIKE '%SECURED 1ST LIEN%' THEN '04. PASSIVE - SECURED 1ST LIEN'
                            WHEN UPPER(Segment) LIKE 'L4605%' THEN '04. PASSIVE - L4605'
                            WHEN UPPER(AssignmentProcess) LIKE '%SLAVE%' THEN '15. SLAVES'
                            WHEN UPPER(AssignmentProcess) LIKE '%LOW DEBT%' THEN '13. LOW DEBT'
                            WHEN UPPER(AssignmentProcess) LIKE '%B0%' THEN '08. BUCKET 0'
                            WHEN UPPER(AssignmentProcess) LIKE '%GEFYRA%' THEN '05. EXCLUSIONS'
                            WHEN UPPER(AssignmentProcess) = 'PASSIVE' AND UPPER(Queue) LIKE '%DEFAULT%' THEN '09. CHECK CASES'
                            ELSE '04. PASSIVE MANAGEMENT'
                         END
	               WHEN UPPER(AssignmentProcess) LIKE 'EXC%' 
	                    OR UPPER(Queue) IN ('N.3869_ΈΝΑΡΞΗ_ΔΙΑΔΙΚΑΣΊΑΣ_ΈΚΠΤΩΣΗΣ', 'ΠΛΗΓΈΝΤΕΣ', 'ΑΝΑΣΤΟΛΉ ΌΧΛΗΣΗΣ BY ASSET OWNER', 'ΑΝΑΣΤΟΛΉ ΌΧΛΗΣΗΣ ΠΛΗΓΈΝΤΩΝ', 'ΑΝΑΣΤΟΛΉ ΌΧΛΗΣΗΣ ALL PORTFOLIOS', 'GEFYRA ACCOUNT DIMOSIOU', 'PLACED BACK CASES TO SELLER', 'ERRONEOUSLY ACTIVATED STANDING ORDERS') 
			          OR UPPER(Queue) LIKE '%DISPUTE%' 
			          OR UPPER(Queue) LIKE '%ΑΝΑΣΤΟΛΉ%' 
			          OR UPPER(Queue) LIKE 'OCW%' THEN '05. EXCLUSIONS'
			     WHEN UPPER(AssignmentProcess) LIKE 'FULF%' 
			          OR UPPER(Queue) IN ('N.3869_ΕΞΟΦΛΉΣΕΙΣ', 'PAID OFF PRIOR MIGRATION') 
			          OR UPPER(Queue) LIKE '%FULFILLED%' 
			          OR UPPER(Queue) LIKE '%ALMOST PAID OFF%' 
			          OR UPPER(StrategyStep) LIKE 'NOP FULFILLED SETT%' THEN '06. FULFILLED SETTLEMENT / PAID OFF'
			     WHEN UPPER(AssignmentProcess) LIKE 'UNH%' AND 
			          (UPPER(Queue) NOT LIKE '%OFFER%' AND UPPER(Queue) NOT LIKE '%COLLECTION%') THEN '07. TO BE ASSIGNED'
			     WHEN UPPER(Queue) LIKE '%TO%ASS%' 
			          OR UPPER(StrategyStep) LIKE 'NOP NON ASSIGN CASES%' THEN '07. TO BE ASSIGNED'
			     WHEN UPPER(Queue) LIKE '%BUCKET00%' 
			          OR UPPER(Queue) LIKE '%B00%' 
			          OR DE.Bucket = 0 
			          OR UPPER(StrategyStep) = 'NOP CURRENT BUCKET 0' 
			          OR UPPER(StrategyStep) LIKE 'NOP SETTLEMENT CB00%' THEN '08. BUCKET 0'
			     WHEN UPPER(Queue) LIKE '%DEFAULT%' 
			          OR UPPER(Queue) LIKE '%RECALL%' 
			          OR UPPER(StrategyStep) LIKE '%NOP%CHECK%' THEN '09. CHECK CASES'
			     WHEN UPPER(Queue) LIKE '%ΕΞΌΦΛΗΣΗ%' THEN '10. CLAIM TO HAVE PAID OFF'
			     WHEN UPPER(Queue) IN ('N3869_ΕΞΑΊΡΕΣΗ_ΔΙΑΧΕΊΡΙΣΗΣ_ΠΑΡΆΠΟΝΑ') 
			          OR UPPER(StrategyStep) = 'COMPLAINT' THEN '11. COMPLAINT'
			     WHEN UPPER(Queue) IN ('CELL_CURRENT BUCKET 01 L3758', 'CELL_SETTLEMENT BUCKET 01 L3758_NON ASSIG', 'CELL_SETTLEMENT BUCKET 01 L3758') 
			          OR UPPER(StrategyStep) = 'NOP L3758' THEN '12. DPD <10'
			     WHEN UPPER(Queue) IN ('CELL_GRACE <10€', 'CELL_GRACE <10€ EXT', 'CELL_GRACE <10€_JUDICIAL', 'CELL_GRACE <10€ EXT_JUDICIAL', 'CELL_GRACE <10€_L3869', 'CELL CB1+ <30€ EXT', 'CELL CB1+ <30€_JUDICIAL', 'CELL CB2-3 <30€', 'CELL CB2-3 <30€_JUDICIAL', 'CELL CB2-3 <30€_L3869') 
			          OR UPPER(StrategyStep) = 'NOP GRACE <50€' THEN '13. LOW DEBT'
			     WHEN UPPER(Queue) IN ('TEST', 'NON ASSIGNED') THEN '14. NOT ASSIGNED'
			     WHEN UPPER(Queue) LIKE '%WDN%' 
			          OR UPPER(Queue) LIKE '%ORPHAN%' 
			          OR UPPER(Queue) LIKE '%SLAVE%' THEN '15. SLAVES'
			     WHEN UPPER(StrategyStep) = 'ZERO PHONES' THEN '16. ZERO PHONES' 
			     WHEN UPPER(StrategyStep) LIKE 'NOP PEND%' 
			          OR UPPER(StrategyStep) = 'NOP AWAITING' THEN '17. PENDING DOCUMENTS OR APPOINTMENT'
			     WHEN UPPER(StrategyStep) LIKE 'NOP % WITH PROMISE' THEN '18. CUSTOMER WITH PROMISE'
			     WHEN UPPER(StrategyStep) LIKE 'NOP%RPC%' 
			          OR UPPER(StrategyStep) LIKE 'NOP HOLD%' THEN '19. ON HOLD - RPC'
			     WHEN UPPER(Queue) IN ('L3869 POS COMMEN DAY', 'L3869 POS NO PAYMENT', 'CELL L3869 POS_TEM_REJ') THEN '20. FINAL L3869 NO CURRENT/FUTURE PAYMENTS'
			     WHEN UPPER(Queue) = 'NEVER ADDED TO PLEDGE REGISTRY' THEN '21. NEVER ADDED TO PLEDGE REGISTRY'
			     ELSE '22. WORKABLE'
			END
   
          -- Pearl Portfolio A
	     WHEN DE.PortfolioName = 'Pearl Portfolio A' THEN 
	          CASE 
                    WHEN DE.AllBorrowersDeceasedFlag = True THEN '01. DECEASED'
	               WHEN OutstandingTotalBalance = 0 THEN '02. BALANCE 0'
                    WHEN UPPER(AssignmentProcess) IS NULL AND UPPER(Segment) IS NULL THEN '03. NO SEGMENT'
                    WHEN 
                        UPPER(AssignmentProcess) LIKE 'PASSIVE%' 
                        OR UPPER(Queue) LIKE '%NO%TREATMENT%' 
                        OR UPPER(Segment) LIKE '%PENDING%' 
                        OR UPPER(Segment) LIKE '%REJEC%>10K%' 
                        OR UPPER(Segment) LIKE '%ACCEPTED%' 
                        OR UPPER(Segment) LIKE '%L4605%' 
                    THEN
                         CASE 
                            WHEN UPPER(Segment) LIKE 'L3869%' AND UPPER(Segment) NOT LIKE '%L3869 REJECTED <10K%' THEN '04. PASSIVE - L3869'
                            WHEN UPPER(AssignmentProcess) LIKE '%NO_SEGMENT%' THEN '04. PASSIVE - NO SEGMENT'
                            WHEN UPPER(Segment) LIKE '%SECURED 1ST LIEN%' THEN '04. PASSIVE - SECURED 1ST LIEN'
                            WHEN UPPER(Segment) LIKE 'L4605%' THEN '04. PASSIVE - L4605'
                            WHEN UPPER(AssignmentProcess) LIKE '%SLAVE%' THEN '15. SLAVES'
                            WHEN UPPER(AssignmentProcess) LIKE '%LOW DEBT%' THEN '13. LOW DEBT'
                            WHEN UPPER(AssignmentProcess) LIKE '%B0%' THEN '08. BUCKET 0'
                            WHEN UPPER(AssignmentProcess) LIKE '%GEFYRA%' THEN '05. EXCLUSIONS'
                            WHEN UPPER(AssignmentProcess) = 'PASSIVE' AND UPPER(Queue) LIKE '%DEFAULT%' THEN '09. CHECK CASES'
                            ELSE '04. PASSIVE MANAGEMENT'
                         END
                    WHEN UPPER(AssignmentProcess) LIKE 'EXC%' 
                         OR UPPER(Queue) IN ('N.3869_ΈΝΑΡΞΗ_ΔΙΑΔΙΚΑΣΊΑΣ_ΈΚΠΤΩΣΗΣ', 'ΠΛΗΓΈΝΤΕΣ', 'ΑΝΑΣΤΟΛΉ ΌΧΛΗΣΗΣ BY ASSET OWNER', 'ΑΝΑΣΤΟΛΉ ΌΧΛΗΣΗΣ ΠΛΗΓΈΝΤΩΝ', 'ΑΝΑΣΤΟΛΉ ΌΧΛΗΣΗΣ ALL PORTFOLIOS', 'GEFYRA ACCOUNT DIMOSIOU', 'PLACED BACK CASES TO SELLER', 'DISPUTE INVESTIGATION', 'ERRONEOUSLY ACTIVATED STANDING ORDERS') 
                         OR UPPER(Queue) LIKE '%DISPUTE%' 
                         OR UPPER(Queue) LIKE '%ΑΝΑΣΤΟΛΉ%' 
                         OR UPPER(Queue) LIKE 'OCW%' THEN '05. EXCLUSIONS'
                    WHEN UPPER(AssignmentProcess) LIKE 'FULF%' 
                         OR UPPER(Queue) IN ('N.3869_ΕΞΟΦΛΉΣΕΙΣ', 'PAID OFF PRIOR MIGRATION') 
                         OR UPPER(Queue) LIKE '%FULFILLED%' 
                         OR UPPER(Queue) LIKE '%ALMOST PAID OFF%' 
                         OR UPPER(StrategyStep) LIKE 'NOP FULFILLED SETT%' THEN '06. FULFILLED SETTLEMENT / PAID OFF'
                    WHEN UPPER(AssignmentProcess) LIKE 'UNH%' AND 
                         (UPPER(Queue) NOT LIKE '%OFFER%' AND UPPER(Queue) NOT LIKE '%COLLECTION%') THEN '07. TO BE ASSIGNED'
                    WHEN UPPER(Queue) LIKE '%TO%ASS%' 
                         OR UPPER(StrategyStep) LIKE 'NOP NON ASSIGN CASES%' THEN '07. TO BE ASSIGNED'
                    WHEN UPPER(Queue) LIKE '%BUCKET00%' 
                         OR UPPER(Queue) LIKE '%B00%' 
                         OR DE.Bucket = 0 
                         OR UPPER(StrategyStep) = 'NOP CURRENT BUCKET 0' 
                         OR UPPER(StrategyStep) LIKE 'NOP SETTLEMENT CB00%' THEN '08. BUCKET 0'
                    WHEN UPPER(Queue) LIKE '%DEFAULT%' 
                         OR UPPER(Queue) LIKE '%RECALL%' 
                         OR UPPER(StrategyStep) LIKE '%NOP%CHECK%' THEN '09. CHECK CASES'
                    WHEN UPPER(Queue) LIKE '%ΕΞΌΦΛΗΣΗ%' THEN '10. CLAIM TO HAVE PAID OFF'
                    WHEN UPPER(Queue) IN ('N3869_ΕΞΑΊΡΕΣΗ_ΔΙΑΧΕΊΡΙΣΗΣ_ΠΑΡΆΠΟΝΑ') 
                         OR UPPER(StrategyStep) = 'COMPLAINT' THEN '11. COMPLAINT'
                    WHEN UPPER(Queue) IN ('PEARL_CURRENT BUCKET 01 L3758', 'PEARL_SETTLEMENT BUCKET 01 L3758_NON ASSIG', 'PEARL_SETTLEMENT BUCKET 01 L3758') 
                         OR UPPER(StrategyStep) = 'NOP L3758' THEN '12. DPD <10'
                    WHEN UPPER(Queue) IN ('PEARL_GRACE <10€', 'PEARL_GRACE <10€ EXT', 'PEARL_GRACE <10€_JUDICIAL', 'PEARL_GRACE <10€ EXT_JUDICIAL', 'PEARL_GRACE <10€_L3869', 'PEARL_GRACE <10€ EXT_L3869', 'PEARL CB1+ <30€ EXT', 'PEARL CB1+ <30€_JUDICIAL', 'PEARL CB2-3 <30€', 'PEARL CB2-3 <30€_JUDICIAL', 'PEARL CB2-3 <30€_L3869') 
                         OR UPPER(StrategyStep) = 'NOP GRACE <50€' THEN '13. LOW DEBT'
                    WHEN UPPER(Queue) IN ('TEST', 'NON ASSIGNED') THEN '14. NOT ASSIGNED'
                    WHEN UPPER(Queue) LIKE '%WDN%' 
                         OR UPPER(Queue) LIKE '%ORPHAN%' 
                         OR UPPER(Queue) LIKE '%SLAVE%' THEN '15. SLAVES'
                    WHEN UPPER(StrategyStep) = 'ZERO PHONES' THEN '16. ZERO PHONES' 
                    WHEN UPPER(StrategyStep) LIKE 'NOP PEND%' 
                         OR UPPER(StrategyStep) = 'NOP AWAITING' THEN '17. PENDING DOCUMENTS OR APPOINTMENT'
                    WHEN UPPER(StrategyStep) LIKE 'NOP % WITH PROMISE' THEN '18. CUSTOMER WITH PROMISE'
                    WHEN UPPER(StrategyStep) LIKE 'NOP%RPC%' 
                         OR UPPER(StrategyStep) LIKE 'NOP HOLD%' THEN '19. ON HOLD - RPC'
                    WHEN UPPER(Queue) IN ('L3869 POS COMMEN DAY', 'L3869 POS NO PAYMENT', 'PEARL L3869 POS_TEM_REJ') THEN '20. FINAL L3869 NO CURRENT/FUTURE PAYMENTS'
                    WHEN UPPER(Queue) = 'NEVER ADDED TO PLEDGE REGISTRY' THEN '21. NEVER ADDED TO PLEDGE REGISTRY'
                    ELSE '22. WORKABLE'
               END
	END AS NonWorkableReason
FROM `{env}-cdh`.derivative.dexposure DE
LEFT JOIN (select distinct 
              segment AS PartySegment, 
              cdi, 
              extrasegment, 
              DeceasedFlag,
              subsegment, 
              businessdate
          from `{env}-cdh`.derivative.dparty 
          where SourceSystem='profits') p
ON	p.CDI =	DE.CDI 
  AND DE.BusinessDate = p.BusinessDate
LEFT JOIN (select distinct CDI, Segment, AssignmentProcess, BusinessDate, ExposureKey
        from `{env}-cdh`.primary.assignment
) ASS 
ON DE.CDI = ASS.CDI 
    AND DE.ExposureKey = ASS.ExposureKey
  AND DE.BusinessDate = ASS.BusinessDate
LEFT JOIN `{env}-cdh`.primary.casecode cc
ON cc.CaseCodeKey = DE.CaseCode
    AND coalesce(DE.ExposureSourceSystemKeyQCCepal, DE.ExposureSourceSystemKeyQCAlpha) = cc.DebtCode
    AND cc.BusinessDate = DE.BusinessDate
LEFT JOIN `{env}-cdh`.landed.qc_case_master cm
ON DE.ExposureKey = cm.old_case_code
AND DE.BusinessDate=cm.BusinessDate
WHERE DE.portfolioname in ('Mercury', 'Light', 'Orbit', 'Cell', 'Pearl Portfolio A')
  AND DE.IsActiveFlag = TRUE				
  AND ASS.AssignmentProcess IS NOT NULL
  AND DE.BusinessDate = {run_date!r}
) 
SELECT 
    ExposureKey,
    PortfolioName
    ,CASE WHEN NonWorkableReason = '21. WORKABLE' OR NonWorkableReason = '22. WORKABLE' THEN true ELSE false END AS WorkableFlag
    ,CASE WHEN NonWorkableReason = '21. WORKABLE' OR NonWorkableReason = '22. WORKABLE' THEN NULL ELSE RIGHT(NonWorkableReason, LEN(NonWorkableReason) - 4) END AS NonWorkableReason
    ,'Daily' as FrequencyType
    ,DebtNumber
    ,BusinessDate
FROM NonWorkableReasonCalculated;
""")

LoanWorkable.createOrReplaceTempView("LoanWorkable")
LoanWorkable.count()

# COMMAND ----------

# MAGIC %md
# MAGIC The ExposureKey 'MRC375537690476005' has ReferenceDate<= 2020 and we do not have it in interaction; that's why we do not give value 'Deceased' to NonWorkableReason. Maybe this case occurs in a lot of cases.  

# COMMAND ----------

import pyspark.sql.functions as F

new_table = spark.sql(f"""
SELECT DISTINCT
    t.ExposureKey,
    t.PortfolioName,
    t.WorkableFlag AS WorkableFlag,
    t.NonWorkableReason AS NonWorkableReason,
    'Daily' AS FrequencyType,
    DebtNumber,
    t.BusinessDate,
    CURRENT_TIMESTAMP() AS IngestionTimestamp
FROM (
        SELECT 
            ExposureKey,
            PortfolioName,
            WorkableFlag,
            NonWorkableReason,
            DebtNumber,
            BusinessDate
        FROM LoanWorkable
        WHERE FrequencyType = 'Daily'
    ) t
""")

new_table.createOrReplaceTempView("new_table")


# COMMAND ----------

if unity == "True":
    new_table.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(derivative_table_path) # in order to write in delta_lake
else:
    new_table.write \
        .format("delta") \
        .mode("append") \
        .save(derivative_table_path) # in order to write in delta_lake

# COMMAND ----------

# ================== Refresh Materialized View (Monthly) ==================
# Refresh only if:
# 1. run_date is the last working day of its month (new month closing)
# 2. run_date belongs to a month that already has data in the MV (re-run scenario)

#last_working_day_check = spark.sql(f"""
#SELECT CurrentMonthLastWorkingDate 
#FROM `{env}-cdh`.reference.ref_calendar 
#WHERE CalendarDate = DATE('{run_date}')
#""").collect()[0]["CurrentMonthLastWorkingDate"]
#
#is_last_working_day = (str(run_date) == str(last_working_day_check))
#
#mv_has_month = spark.sql(f"""
#SELECT COUNT(*) AS cnt
#FROM `{env}-cdh`.derivativeactive.dportfoliotoolthirdpartiesmonthly
#WHERE BusinessDate >= DATE_TRUNC('MONTH', DATE('{run_date}'))
#LIMIT 1
#""").collect()[0]["cnt"] > 0
#
#if is_last_working_day or mv_has_month:
#    print(f"Refreshing MV (is_last_working_day={is_last_working_day}, mv_has_month={mv_has_month})...")
#    spark.sql(f"REFRESH MATERIALIZED VIEW `{env}-cdh`.derivativeactive.dportfoliotoolthirdpartiesmonthly")
#    print("Materialized view refreshed successfully.")
#else:
#    print(f"run_date {run_date} does not affect MV. Skipping refresh.")
