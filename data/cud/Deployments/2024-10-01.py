# Databricks notebook source
# MAGIC %md
# MAGIC #### D360-1857
# MAGIC #### Changes on values of field PartyRelationshipType of Exposure cup table (for profits & arotron source systems) 

# COMMAND ----------

import os
env = os.environ["S3PREFIX"]

spark.sql(f'''
            UPDATE `{env}-ec1-cepal-curated-area`.exposure
            SET PartyRelationshipType = 'Primary Owner'
            WHERE SourceSystem in ('profits','arotron')
            and PartyRelationshipType = 'Main Beneficiary';
          ''')

spark.sql(f'''
            UPDATE `{env}-ec1-cepal-curated-area`.exposure
            SET PartyRelationshipType = 'Co-Owner'
            WHERE SourceSystem in ('profits','arotron')
            and PartyRelationshipType = 'Other Beneficiaries';
          ''')

# COMMAND ----------

# MAGIC %md
# MAGIC #### D360-1857_new
# MAGIC * Re-run adhoc dexposure for every businessdate
# MAGIC * Run the following scripts

# COMMAND ----------

spark.sql(f'''
            UPDATE `p-cdh`.derivative.dexposure
            SET PartyRelationshipType = 'Primary Owner'
            WHERE PartyRelationshipType = 'Main Beneficiary';
          ''')

spark.sql(f'''
            UPDATE `p-cdh`.derivative.dexposure
            SET PartyRelationshipType = 'Co-Owner'
            WHERE PartyRelationshipType = 'Other Beneficiaries';
          ''')

spark.sql(f'''
            ALTER TABLE `p-cdh`.derivative.dexposure
            DROP COLUMN AssetClass, AssetClassCode;
          ''')

spark.sql(f'''
          ALTER TABLE `p-cdh`.derivative.dexposure
          ADD COLUMN InitialAssignmentAmount decimal(19,2) AFTER OriginationAmount;
          ''')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Update scripts for primary.archive

# COMMAND ----------

spark.sql(f'''
            UPDATE `p-cdh`.primaryarchive.exposure
            SET PartyRelationshipType = 'Primary Owner'
            WHERE SourceSystem in ('profits','arotron')
            and PartyRelationshipType = 'Main Beneficiary';
          ''')

spark.sql(f'''
            UPDATE `p-cdh`.primaryarchive.exposure
            SET PartyRelationshipType = 'Primary Owner'
            WHERE SourceSystem in ('profits','arotron')
            and PartyRelationshipType = 'Main Beneficiary';
          ''')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Update scrips for primary.active
# MAGIC

# COMMAND ----------

spark.sql(f'''
            UPDATE `p-cdh`.primaryactive.exposure
            SET PartyRelationshipType = 'Primary Owner'
            WHERE SourceSystem in ('profits','arotron')
            and PartyRelationshipType = 'Main Beneficiary';
          ''')

spark.sql(f'''
            UPDATE `p-cdh`.primaryactive.exposure
            SET PartyRelationshipType = 'Primary Owner'
            WHERE SourceSystem in ('profits','arotron')
            and PartyRelationshipType = 'Main Beneficiary';
          ''')

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Update scripts for derivative.active

# COMMAND ----------

spark.sql(f'''
            UPDATE `p-cdh`.derivativeactive.dexposure
            SET PartyRelationshipType = 'Primary Owner'
            WHERE PartyRelationshipType = 'Main Beneficiary';
          ''')

spark.sql(f'''
            UPDATE `p-cdh`.derivativeactive.dexposure
            SET PartyRelationshipType = 'Co-Owner'
            WHERE PartyRelationshipType = 'Other Beneficiaries';
          ''')
          
spark.sql(f'''
            ALTER TABLE `p-cdh`.derivativeactive.dexposure
            DROP COLUMN AssetClass, AssetClassCode;
          ''')

spark.sql(f'''
          ALTER TABLE `p-cdh`.derivativeactive.dexposure
          ADD COLUMN InitialAssignmentAmount decimal(19,2) AFTER OriginationAmount;
          ''')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Update scripts for derivative.archive

# COMMAND ----------

spark.sql(f'''
            UPDATE `p-cdh`.derivativearchive.dexposure
            SET PartyRelationshipType = 'Primary Owner'
            WHERE PartyRelationshipType = 'Main Beneficiary';
          ''')

spark.sql(f'''
            UPDATE `p-cdh`.derivativearchive.dexposure
            SET PartyRelationshipType = 'Co-Owner'
            WHERE PartyRelationshipType = 'Other Beneficiaries';
          ''')

spark.sql(f'''
            ALTER TABLE `p-cdh`.derivativearchive.dexposure
            DROP COLUMN AssetClass, AssetClassCode;
          ''')

spark.sql(f'''
          ALTER TABLE `p-cdh`.derivativearchive.dexposure
          ADD COLUMN InitialAssignmentAmount decimal(19,2) AFTER OriginationAmount;
          ''')
