# Databricks notebook source
# MAGIC %md
# MAGIC #### D360-1175
# MAGIC * Addition of a new field : CepalEligibilityDate - timestamp in DUnderLaw.
# MAGIC Dropped completely the keys UnderLawKey & UnderLawId from DUnderLaw
# MAGIC Re-implemented the way that DUnderLawId & DUnderLawKey were calculated.
# MAGIC * Changed DExposure's ExposureStatus values in order to contain only latin characters.
# MAGIC Hotfixes in both tables of DUnderLaw & Dexposure

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE `p-cdh`.derivative.dunderlaw
# MAGIC ADD COLUMN CepalEligibilityDate timestamp AFTER AppealCourtDate;
# MAGIC
# MAGIC ALTER TABLE `p-cdh`.derivative.dunderlaw
# MAGIC DROP COLUMN UnderLawId;
# MAGIC
# MAGIC ALTER TABLE `p-cdh`.derivative.dunderlaw
# MAGIC DROP COLUMN UnderLawKey;
# MAGIC
# MAGIC UPDATE `p-cdh`.derivative.dexposure
# MAGIC SET ExposureStatus = 
# MAGIC     CASE WHEN ExposureStatusCode="1"  OR ExposureStatusCode="074-NON DELINQUENT" THEN "Normal"
# MAGIC          WHEN ExposureStatusCode="2"  OR ExposureStatusCode="074-DELINQUENT" THEN "Overdue"
# MAGIC          WHEN ExposureStatusCode="3"  OR ExposureStatusCode="074-DENOUNCED" THEN "Define Delay"
# MAGIC          WHEN ExposureStatusCode="4"  OR ExposureStatusCode="074-WRITE OFF" THEN "Writen Off"
# MAGIC          END

# COMMAND ----------

# MAGIC %md
# MAGIC #### D360-1794
# MAGIC * Update code for DParty & DPfFinalOutput

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE `p-cdh`.derivative.dparty
# MAGIC ADD COLUMN OCWFlag boolean AFTER L3869InterBankingStatus;
# MAGIC
# MAGIC ALTER TABLE `p-cdh`.derivative.dparty
# MAGIC ADD COLUMN OCWStatus string AFTER OCWFlag;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE `p-cdh`.derivative.dpffinaloutput
# MAGIC ADD COLUMN AgreementKey string AFTER PrimaryPartyCDI

# COMMAND ----------

# MAGIC %md
# MAGIC * Re-run for every BusinessDate : DUnderLaw, DParty
# MAGIC * Run in prod : DPfFinalOutput
