# Databricks notebook source
# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import warnings
warnings.filterwarnings("ignore")

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p /dbfs/mnt/FileStore/solution_accelerators/digitization/ && touch /dbfs/mnt/FileStore/solution_accelerators/digitization/init.sh
# MAGIC cat <<EOF > /dbfs/mnt/FileStore/solution_accelerators/digitization/init.sh
# MAGIC #!/usr/bin/env bash
# MAGIC sudo apt-get install -y tesseract-ocr
# MAGIC sudo apt-get install -y poppler-utils
# MAGIC EOF

# COMMAND ----------

# Set mlflow experiment explicitly to make sure the code runs in both interactive execution and job execution
import mlflow
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
mlflow.set_experiment('/Users/{}/gemi_document_digitization'.format(username))
model_name = 'pdf_parser'

# COMMAND ----------

# Set sector to include brewing companies
sector = 'i22' 

# Here we use a `/tmp/...` path in DBFS to minimize dependency. We recommend using a `/mnt/...` path or one that directly connects to your cloud storage for production usage. To learn more about mount points, please review [this document](https://docs.databricks.com/dbfs/mounts.html).  If you would like to use a mount point or a different path, please update the variable below with the appropriate path:
landing_zone = '/Volumes/d-gemi/default/digitization_experiment' 
k = 3
landing_zone_fs = '{}/**/pages'.format(landing_zone)

# COMMAND ----------

print(landing_zone_fs)
