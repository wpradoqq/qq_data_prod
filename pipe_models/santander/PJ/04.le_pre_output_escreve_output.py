# Databricks notebook source
# MAGIC %run "/Shared/common-notebooks/dataprep-funcoes/initial-func"

# COMMAND ----------

import os
import datetime

# COMMAND ----------

pre_outputpath = '/mnt/ml-prd/ml-data/propensaodeal/santander/outputPJ'
pre_outputpath_dbfs = '/dbfs/mnt/ml-prd/ml-data/propensaodeal/santander/outputPJ'

santander_output = '/mnt/ml-prd/ml-data/propensaodeal/santander/output'

# COMMAND ----------

'/dbfs/mnt/ml-prd/ml-data/propensaodeal/santander/outputPJ/pre_output:Base_Santander_PJ_QQ_Model_2022-06-01.csv'


# COMMAND ----------

for file in os.listdir(pre_outputpath_dbfs):
  print ('file:',file)
  
date = file.split('.')[0]
date = date.split('_')[6]
print ('date:',date)

createdAt = datetime.datetime.today().date()
print ('createdAt:', createdAt)

# COMMAND ----------

output = spark.read.format('csv').option('header','True').load(os.path.join(pre_outputpath, file))
output = output.drop('_c0')
output = output.withColumn('Document', F.lpad(F.col("Chave"),14,'0'))
output = output.groupBy(F.col('Document')).agg(F.max(F.col('GH')), F.max(F.col('P_1')), F.avg(F.col('P_1')))
output = output.withColumn('Provider', F.lit('qq_santander_PJ_propensity_v1'))
output = output.withColumn('Date', F.lit(date))
output = output.withColumn('CreatedAt', F.lit(createdAt))
output = changeColumnNames(output, ['Document','Score','ScoreValue','ScoreAvg','Provider','Date','CreatedAt'])

# COMMAND ----------

display(output)

# COMMAND ----------

output.coalesce(1).write.mode('overwrite').options(header='True', delimiter=';').csv(pre_outputpath+'/tmp')

for files in dbutils.fs.ls(pre_outputpath+'/tmp'):
  if files.name.split('.')[-1] == 'csv':
    dbutils.fs.cp(files.path, pre_outputpath+'/santander_pj_model_to_production_'+str(createdAt)+'.csv')
    dbutils.fs.rm(pre_outputpath+'/tmp', recurse=True)
    dbutils.fs.rm(pre_outputpath+'/'+file, recurse=True)

# COMMAND ----------

dbutils.fs.cp(pre_outputpath+'/santander_pj_model_to_production_'+str(createdAt)+'.csv', santander_output+'/santander_pj_to_production_'+str(createdAt)+'.csv')

# COMMAND ----------


