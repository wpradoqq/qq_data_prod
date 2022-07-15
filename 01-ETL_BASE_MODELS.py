# Databricks notebook source
import time
time.sleep(300)

# COMMAND ----------

# MAGIC %run "/Shared/common-notebooks/dataprep-funcoes/initial-func"

# COMMAND ----------

import zipfile
import datetime
date = datetime.datetime.today() - datetime.timedelta(days=1)
ArqDate = str(date.year)+str(date.month).zfill(2)+str(date.day).zfill(2)
date = str(date.year)+'-'+str(date.month).zfill(2)+'-'+str(date.day).zfill(2)

spark.conf.set('spark.sql.caseSensitive', True)

#date = '2022-07-15'
#ArqDate = '20220715'

date, ArqDate

# COMMAND ----------

# DBTITLE 1,Diretórios
##Diretorios
caminho_base_dbfs = '/dbfs/mnt/ml-prd/ml-data/base_models'
caminho_base = '/mnt/ml-prd/ml-data/base_models'
caminho_export = '/mnt/bi-reports/export_full/person_without_project/'
caminho_export_dbfs = '/dbfs/mnt/bi-reports/export_full/person_without_project/'
#caminho_export = '/mnt/bi-reports/WP/PERSON_ZIP/'
#caminho_export_dbfs = '/dbfs/mnt/bi-reports/WP/PERSON_ZIP/'
caminho_export_zip = '/mnt/bi-reports/export_full/person/'
caminho_export_zip_dbfs = '/dbfs/mnt/bi-reports/export_full/person/'

# COMMAND ----------

# DBTITLE 1,Unzip Person
#arquivos_origin_ref = [file.name for file in dbutils.fs.ls(os.path.join(caminho_export_zip, ArqDate))]
#  
#for file in arquivos_origin_ref:
#  with zipfile.ZipFile(os.path.join(caminho_export_zip_dbfs, ArqDate, file), "r") as arquivozipado:
#      try:
#        arquivozipado.extractall(path = (os.path.join(caminho_export_dbfs, ArqDate)))
#      except Exception as e:
#        print (e)
#
#qtd_arquivos = len(arquivos_origin_ref)
#                                 
#arquivos_unzip_ref = [file.name for file in dbutils.fs.ls((os.path.join(caminho_export_zip, ArqDate)))]
#print ('deszipados', len(arquivos_unzip_ref), 'de', qtd_arquivos, 'arquivos com sucesso')

# COMMAND ----------

# DBTITLE 1,Schema person
spark.conf.set('spark.sql.caseSensitive', True)

DATA_PATH = "/dbfs/SCHEMAS/DATA_SCHEMA.json"
with open(DATA_PATH) as f:
  SCHEMA = T.StructType.fromJson(json.load(f))

#SCHEMA = spark.read\
#            .format( "com.mongodb.spark.sql.DefaultSource")\
#            .option('spark.mongodb.input.sampleSize', 50000)\
#            .option("database", "qq")\
#            .option("spark.mongodb.input.collection", "col_person")\
#            .option("badRecordsPath", "/tmp/badRecordsPath")\
#            .load().limit(1).schema

# COMMAND ----------

# DBTITLE 1,Simulação
#PYSPARK
SCHEMA_SIMU = spark.read\
        .format("com.mongodb.spark.sql.DefaultSource")\
        .option('spark.mongodb.input.database', "qq")\
        .option('spark.mongodb.input.collection', "col_simulation")\
        .load().limit(1).schema


DATA_Simu = spark.read.schema(SCHEMA_SIMU)\
        .format("com.mongodb.spark.sql.DefaultSource")\
        .option('spark.mongodb.input.database', "qq")\
        .option('spark.mongodb.input.collection', "col_simulation")\
        .load()\
        .drop('offers')
#display(DATA)

# COMMAND ----------

DATA_Simu.write.mode('overwrite').format('delta').save(os.path.join(caminho_base, 'Simulation',date))


# COMMAND ----------

# DBTITLE 1,Autenticação
#PYSPARK
SCHEMA_AUTH = spark.read\
        .format("com.mongodb.spark.sql.DefaultSource")\
        .option('spark.mongodb.input.database', "qq")\
        .option('spark.mongodb.input.collection', "col_authentication")\
        .load().limit(1).schema


DATA_Auth = spark.read.schema(SCHEMA_AUTH)\
        .format("com.mongodb.spark.sql.DefaultSource")\
        .option('spark.mongodb.input.database', "qq")\
        .option('spark.mongodb.input.collection', "col_authentication")\
        .load()
#display(dfAuth)

# COMMAND ----------

DATA_Auth.write.mode('overwrite').format('delta').save(os.path.join(caminho_base, 'Authentication',date))


# COMMAND ----------

# DBTITLE 1,Info
dfInfo = spark.read.schema(SCHEMA).json(caminho_export+ArqDate+'/').distinct()\
.select('_id','document','documentType', F.col('info.phones.tags').alias('tag_telefone'), F.col('info.phones.phone').alias('phone'),\
        F.col('info.addresses.state').alias('state'), F.col('info.addresses.city').alias('city'),\
        F.col('info.addresses.zipcode').alias('zipcode'), F.col('info.phones.areaCode').alias('DDD'),\
        F.col('info.emails.email').alias('email'), F.col('info.emails.tags').alias('tag_email'), F.col('info.emails.domain').alias('domain'),\
        F.col('info.emails.dataProvider').alias('dataProvider'),F.col('info.emails.validators').alias('validators'),\
        F.col('credentials.phone').alias('credential_phone'))



# COMMAND ----------

dfInfo.write.format('delta').mode('overwrite').save(os.path.join(caminho_base, 'Info',date))

# COMMAND ----------

# DBTITLE 1,Divida
dfDebts = spark.read.schema(SCHEMA).json(caminho_export+ArqDate+'/').distinct()\
.select('_id', 'document','documentType', F.col('debts.contract').alias('contract'),F.col('debts.portfolio').alias('portfolio'),\
        F.col('debts.originalAmount').alias('originalAmount'), F.col('debts.product').alias('product'),\
        F.col('debts.tags').alias('tag_divida'), F.col('debts.creditor').alias('creditor'),\
        F.col('debts.dueDate').alias('dueDate'),F.col('debts.status').alias('status'))\
.withColumn('debts',F.arrays_zip('contract','portfolio','originalAmount', 'product', 'creditor', 'tag_divida', 'dueDate', 'status'))\
.select('_id', 'documentType', 'document', F.explode('debts').alias('debts'))\
.select('_id', 'documentType', 'document','debts.contract', 'debts.portfolio','debts.originalAmount', 'debts.product', 'debts.tag_divida',\
        'debts.creditor', F.to_date(F.col('debts.dueDate')[0:10], 'yyyy-M-d').alias('INICIO_DIVIDA'), 'debts.status')


#dfDebts.printSchema()

# COMMAND ----------

#Salvando arquivo de PF
dfDebtsPF = dfDebts.filter(F.col('documentType')=='cpf').drop('documentType')

dfDebtsPF.write.mode('overwrite').format('delta').save(os.path.join(caminho_base, 'Debts/CPF',date))


# COMMAND ----------

#Salvando arquivo de PJ
dfDebtsPJ = dfDebts.filter(F.col('documentType')=='cnpj').drop('documentType')

dfDebtsPJ.write.mode('overwrite').format('delta').save(os.path.join(caminho_base, 'Debts/CNPJ',date))


# COMMAND ----------

spark.sql("OPTIMIZE delta.`{0}` ZORDER BY (_id)".format(os.path.join(os.path.join(caminho_base, 'Debts/CNPJ',date))))

# COMMAND ----------

# DBTITLE 1,Acordos
dfdeals = spark.read.schema(SCHEMA).json(caminho_export+ArqDate+'/').distinct()\
.select('_id','document',F.col('deals._id').alias('dealID'),F.col('deals.creditor').alias('creditor'),\
        F.col('deals.createdAt').alias('createdAt'), F.col('deals.status').alias('status'))\
.withColumn('deals', F.arrays_zip('dealID','creditor','createdAt', 'status'))\
.select('_id','document', F.explode('deals').alias('deals'))\
.select('_id','document','deals.dealID','deals.creditor',F.to_date(F.col('deals.createdAt')[0:10], 'yyyy-M-d').alias('createdAt'),'deals.status')


#dfdeals = dfdeals.orderBy('creditor',F.col('createdAt').desc()).dropDuplicates(['document','creditor'])

# COMMAND ----------

dfdeals.write.mode('overwrite').format('delta').save(os.path.join(caminho_base, 'Deals', date))

# COMMAND ----------

os.path.join(caminho_base, 'Deals', date)

# COMMAND ----------

# DBTITLE 1,Pagamentos
dfinstallments = spark.read.schema(SCHEMA).json(caminho_export+ArqDate+'/').distinct()\
                 .select('_id','document',F.col('installments.dealID').alias('dealID'),F.col('installments.creditor').alias('creditor'),\
                         F.col('installments.createdAt').alias('createdAt'), F.col('installments.payment.paidAt').alias('paidAt'),\
                         F.col('installments.dueAt').alias('dueAt'), F.col('installments.status').alias('status'),\
                         F.col('installments.payment.paidAmount').alias('paidAmount'),\
                         F.col('installments.installmentAmount').alias('installmentAmount'), F.col('installments.installment').alias('installment'))\
                 .withColumn('installments', F.arrays_zip('dealID','creditor','createdAt', 'paidAt', 'dueAt', 'status','paidAmount', 'installmentAmount', 'installment'))\
                 .select('_id','document', F.explode('installments').alias('installments'))\
                 .select('_id','document','installments.dealID','installments.creditor','installments.createdAt', 'installments.paidAt','installments.dueAt',\
                         'installments.status', 'installments.paidAmount','installments.installmentAmount','installments.installment')

aux = dfinstallments.groupBy(F.col('dealID')).agg((F.count('dealID').alias('QTD_PARCELAS')), F.round(F.sum('installmentAmount'),2).alias('VALOR_ACORDO'))
dfinstallments = dfinstallments.filter((F.col('status')=='paid') & (F.col('installment')==1)).drop(*['status','installment', 'installmentAmount', 'paidAmount'])
dfinstallments = dfinstallments.join(aux, on = 'dealID', how = 'inner')

del aux

# COMMAND ----------

spark.sql("OPTIMIZE delta.`{0}`".format(os.path.join(os.path.join(caminho_base, 'Authentication',date))))

# COMMAND ----------

spark.sql("OPTIMIZE delta.`{0}`".format(os.path.join(os.path.join(caminho_base, 'Simulation',date))))

# COMMAND ----------

spark.sql("OPTIMIZE delta.`{0}` ZORDER BY (_id)".format(os.path.join(caminho_base, 'Info',date)))

# COMMAND ----------

spark.sql("OPTIMIZE delta.`{0}` ZORDER BY (_id)".format(os.path.join(os.path.join(caminho_base, 'Debts/CPF',date))))

# COMMAND ----------

spark.sql("OPTIMIZE delta.`{0}` ZORDER BY (_id)".format(os.path.join(os.path.join(caminho_base, 'Deals', date))))

# COMMAND ----------

spark.sql("OPTIMIZE delta.`{0}` ZORDER BY (_id)".format(os.path.join(os.path.join(caminho_base, 'Installments', date))))
