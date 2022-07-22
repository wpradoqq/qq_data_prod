# Databricks notebook source
import time
time.sleep(300)

# COMMAND ----------

import datetime
ArqDate = datetime.datetime.today() - datetime.timedelta(days=1)
date = datetime.datetime.today()
ArqDate = str(ArqDate.year)+'-'+str(ArqDate.month).zfill(2)+'-'+str(ArqDate.day).zfill(2)
date = str(date.year)+'-'+str(date.month).zfill(2)+'-'+str(date.day).zfill(2)

#date = '2022-07-14'
#ArqDate = '2022-07-14'
date, ArqDate

# COMMAND ----------

try:
  dbutils.widgets.remove('ARQUIVO_ESCOLHIDO')
except:
  pass

# COMMAND ----------

# DBTITLE 1,Carregamento de funções pré-definidas
# MAGIC %run "/Shared/common-notebooks/dataprep-funcoes/initial-func"

# COMMAND ----------

from pyspark.sql.functions import substring
from pyspark.sql.functions import desc
import os
import datetime
import zipfile

# COMMAND ----------

# DBTITLE 1,Definição dos diretórios do blob
# Todos realizados de forma manual
prefix = "etl/pag/processed"

caminho_base = '/mnt/qq-integrator/etl/pag/processed'
caminho_base_dbfs = '/dbfs/mnt/qq-integrator/etl/pag/processed'
caminho_trusted = '/mnt/ml-prd/ml-data/propensaodeal/pag/trusted'
caminho_trusted_dbfs = '/dbfs/mnt/ml-prd/ml-data/propensaodeal/pag/trusted'
caminho_base_models = '/mnt/ml-prd/ml-data/base_models'

file_name = 'Base_Pag_QQ_Model_'+date+'.csv'

# COMMAND ----------

dbutils.widgets.dropdown('processamento', 'AUTO', ['AUTO', 'MANUAL'])
if dbutils.widgets.get('processamento') == 'AUTO':
  processo_auto = True
else:
  processo_auto = False
  
processo_auto

files = {}
fileList = []
for file in os.listdir(caminho_base_dbfs):
  if 'integracaoQUEROQUITAR' in file.split('.')[0]:
    files.update({int(file.split('_')[-1].split('.')[0]):file})
    fileList.append(file)
max_date = max(files)
max_file = files[max_date]

if processo_auto:
  try:
    dbutils.widgets.remove('ARQUIVO_ESCOLHIDO')
  except:
    pass
  arquivo_escolhido = max_file
else:
  dbutils.widgets.dropdown('ARQUIVO_ESCOLHIDO', max_file, fileList)
  arquivo_escolhido = dbutils.widgets.get('ARQUIVO_ESCOLHIDO')
  
print ('processo_auto', processo_auto)
arquivo_escolhido

# COMMAND ----------

fileDate = arquivo_escolhido.split('_')[-1].split('.')[0]
fileDate = fileDate[0:-4]

fileDate

# COMMAND ----------

df = spark.read.option('delimiter',',').option('header', 'True').csv(os.path.join(caminho_base,arquivo_escolhido))

# COMMAND ----------

df = df.filter(F.col('DATA PAGAMENTO').isNull()).drop(*['DATA PAGAMENTO','VALOR PAGO'])

df = df.withColumnRenamed('NOME CLIENTE', 'NOME_CLIENTE')\
.withColumnRenamed('DIAS ATRASO', 'DIAS_ATRASO')\
.withColumnRenamed('DATA DIVIDA', 'DATA_DIVIDA')\
.withColumnRenamed('DATA ULTIMA FATURA', 'DATA_ULTIMA_FATURA')\
.withColumnRenamed('DATA INTERROMPE COBRANCA', 'DATA_INTERROMPE_COBRANCA')\
.withColumnRenamed('DATA RETORNO COBRANCA', 'DATA_RETORNO_COBRANCA')\
.withColumnRenamed('VALOR ULTIMA FATURA', 'VALOR_ULTIMA_FATURA')


# COMMAND ----------

display(df)

# COMMAND ----------

# DBTITLE 1,Info
dfInfo = spark.read.format('delta').load(os.path.join(caminho_base_models, 'Info',ArqDate)).filter(F.col('documentType')=='cpf')

dfInfo = dfInfo.select('_id','document','documentType','tag_telefone','state','tag_email','domain')


listTel= ['skip:hot','skip:alto','skip:medio','skip:baixo','skip:nhot','sem_tags']
listMail= ['skip:hot','skip:alto','skip:medio','skip:baixo','skip:nhot','sem_tags']

for item in range(10):
  for i in listTel:
    if 'sem_tags' in i:
      dfInfo = dfInfo.withColumn('Telefone{0}_{1}'.format(item+1,i),F.when(F.array_contains(F.col('tag_telefone')[item], i).isNull(), True).otherwise(False))
    else:
      dfInfo = dfInfo.withColumn('Telefone{0}_{1}'.format(item+1,i),F.when(F.array_contains(F.col('tag_telefone')[item], i) == True, True).otherwise(False))
      
dfInfo = dfInfo.drop('tag_telefone')

for item in range(3):
  for i in listMail:
    if 'sem_tags' in i:
      dfInfo = dfInfo.withColumn('Email{0}_{1}'.format(item+1,i),F.when(F.array_contains(F.col('tag_email')[item], i).isNull(), True).otherwise(False))
    else:
      dfInfo = dfInfo.withColumn('Email{0}_{1}'.format(item+1,i),F.when(F.array_contains(F.col('tag_email')[item], i) == True, True).otherwise(False))
  dfInfo = dfInfo.withColumn('Email{0}_dominio'.format(item+1),F.col('domain')[item])
  
dfInfo = dfInfo.drop('tag_email')
dfInfo = dfInfo.drop('domain')

for item in range(3):
  dfInfo = dfInfo.withColumn('UF{0}'.format(item+1,i),F.upper(F.when(F.col('state')[item] == '!!!<<Cadastrar>>!!!', None).otherwise(F.col('state')[item])))
  
dfInfo = dfInfo.drop('state').orderBy(F.col('_id').asc())

# COMMAND ----------

# DBTITLE 1,Debts
dfDebts = spark.read.format('delta').load(os.path.join(caminho_base_models, 'Debts/CPF',ArqDate))

listDiv = ['rank:a','rank:b','rank:c','rank:d','rank:e', 'sem_tags']

for i in listDiv:
  if 'sem_tags' in i:
    dfDebts = dfDebts.withColumn('SkipDivida_{0}'.format(i),F.when(F.array_contains(F.col('tag_divida'), i).isNull(), True).otherwise(False))
  else:
    dfDebts = dfDebts.withColumn('SkipDivida_{0}'.format(i),F.when(F.array_contains(F.col('tag_divida'), i) == True, True).otherwise(False))
    
dfDebts = dfDebts.drop('tag_divida')

dfDebts = dfDebts.distinct()

dfDebts = dfDebts.join(dfDebts.groupBy('_id','creditor').agg(F.min('INICIO_DIVIDA').alias('INICIO_DIVIDA')),on=(['_id','creditor','INICIO_DIVIDA']),how='leftsemi')

# COMMAND ----------

##Filtrando apenas dividas do credor
dfDebtsCre = dfDebts.filter(F.col('creditor') == 'pag').orderBy(F.col('_id').asc())
dfValCredor =  dfDebtsCre.select('_id','contract')

##Criando DF de divida nos demais credores
dfDebtsOut = dfDebts.filter(F.col('creditor') != 'pag').select('_id','creditor').orderBy(F.col('_id').asc())
dfDebtsOut = dfDebtsOut.dropDuplicates(['_id'])

#Join dos DFs de Info e Debts do credor
dfBase = dfInfo.join(dfDebtsCre.drop('document'), on='_id').orderBy(F.col('document').asc(),F.col('contract').asc())

##Join do arquivo ETL com os dados do mongo
dfArq = df.join(dfBase, (df.CONTA == dfBase.contract) & (df.CPF == dfBase.document), 'left')
dfArq = dfArq.drop('creditor').drop('document').drop('originalAmoun').drop('contract')

#Join do DF completo com o DF de divida em outros credores e criando FLAG
dfArq = dfArq.join(dfDebtsOut, on= '_id', how='left')
dfArq = dfArq.withColumn('Divida_Outro_Credor', F.when(F.col('creditor').isNull(),False).otherwise(True)).drop('creditor')

##Criando Flag de outras dividas no mesmo credor
dfValCredor = dfValCredor.join(dfArq, (dfValCredor.contract == dfArq.CONTA) & (dfValCredor._id == dfArq._id ), 'left').select(dfValCredor._id,'contract','CONTA').filter(F.col('CONTA').isNull()).drop('CONTA').orderBy(F.col('_id').asc())
dfArq = dfArq.join(dfValCredor, on='_id', how='left')
dfArq = dfArq.withColumn('Divida_Mesmo_Credor', F.when(F.col('contract').isNull(),False).otherwise(True)).drop(*['contract','CONTA'])

dfArq = dfArq.distinct()

# COMMAND ----------

# DBTITLE 1,Simulação
DATA_Simu = spark.read.format('delta').load(os.path.join(caminho_base_models, 'Simulation',ArqDate))

# COMMAND ----------

dfSimu = DATA_Simu.select('personID','creditor','createdAt')\
        .filter(F.col('creditor')=='pag')\
        .filter(F.col('createdAt') <= (F.to_date(F.lit(fileDate), 'yyyyMMdd'))).orderBy(F.col('createdAt').desc())

##Contagem de simulações por CPF
dfSimuQtd = dfSimu.groupBy('personID').count()\
            .select(F.col('personID').alias('_id'), F.col('count').alias('QTD_SIMU')).orderBy(F.col('_id').asc())

#display(dfSimu)

# COMMAND ----------

##Criando coluna de quantidade de simulações por CPF no DF principal
dfArq = dfArq.join(dfSimuQtd, on= '_id', how = 'left')\
        .withColumn('QTD_SIMU', (F.when(F.col('QTD_SIMU').isNull(), 0).otherwise(F.col('QTD_SIMU'))))

# COMMAND ----------

# DBTITLE 1,Autenticação
DATA_Auth = spark.read.format('delta').load(os.path.join(caminho_base_models, 'Authentication',ArqDate))

# COMMAND ----------

##Filtrando colunas de autenticações
dfAuth = DATA_Auth.select('tokenData.document','tokenData.contact','tokenData.contract','tokenData.creditor','createdAt')\
        .filter(F.col('creditor')=='pag')\
        .filter(F.col('createdAt') <= (F.to_date(F.lit(fileDate), 'yyyyMMdd'))).orderBy(F.col('createdAt').desc())


##Contagem de autenticação por CPF
dfAuthQtd = dfAuth.groupBy('document').count()\
            .select(F.col('document').alias('CPF'), F.col('count').alias('QTD_AUTH')).orderBy(F.col('CPF').asc())

#display(dfAuth)

# COMMAND ----------

dfArq = dfArq.orderBy(F.col('CPF').asc())

##Criando coluna de quantidade de autenticações por CPF no DF principal
dfArq = dfArq.join(dfAuthQtd, on= 'CPF', how = 'left')\
        .withColumn('QTD_AUTH', (F.when(F.col('QTD_AUTH').isNull(), 0).otherwise(F.col('QTD_AUTH'))))

# COMMAND ----------

# DBTITLE 1,Acordos
#Buscando todos acordos do credor
dfdeals = spark.read.format('delta').load(os.path.join(caminho_base_models, 'Deals', ArqDate)).filter(F.col('creditor')=='pag')

dfdeals = dfdeals.filter((F.col('status')!='error') & (F.col('createdAt')>='2020-01-01'))\
          .select(F.col('document').alias('CPF'),F.col('createdAt').alias('DT_ACORDO'),'dealID','status')\
          .withColumn('VARIAVEL_RESPOSTA_ACORDO', F.lit(True))

#dfdeals.display()

# COMMAND ----------

#Buscando todos acordos do credor
dfArq = dfArq.join(dfdeals, on=('CPF'), how= 'left')
dfArq = dfArq.withColumn('VARIAVEL_RESPOSTA_ACORDO', (F.when(F.col('VARIAVEL_RESPOSTA_ACORDO').isNull(), False).otherwise(F.col('VARIAVEL_RESPOSTA_ACORDO'))))

# COMMAND ----------

# DBTITLE 1,Pagamentos
dfinstallments = spark.read.format('delta').load(os.path.join(caminho_base_models, 'Installments', ArqDate))\
                 .withColumn('DT_PAGAMENTO', F.when(F.col('paidAt').isNull(),F.col('dueAt')[0:10]).otherwise(F.col('paidAt'))[0:10])\
                 .filter((F.col('DT_PAGAMENTO')>='2020-01-01') & (F.col('creditor')=='pag'))\
                 .select('DT_PAGAMENTO', 'dealID', 'QTD_PARCELAS') 
#display(dfinstallments)

# COMMAND ----------

#Criando variavel resposta de pagamento
dfArq = dfArq.join(dfinstallments, on = 'dealID', how= 'left')
dfArq = dfArq.withColumn('PAGTO', (F.when(F.col('DT_PAGAMENTO').isNull(), False).otherwise(True)))\
.withColumn('QTD_PARCELAS', (F.when(F.col('QTD_PARCELAS').isNull(), 0).otherwise(F.col('QTD_PARCELAS'))))

# COMMAND ----------

# DBTITLE 1,Interações
dfinteractions = spark.read.format('delta').load(os.path.join('/mnt/bi-reports/', 'WP/Analises/Gustavo', 'interaction_p1'))\
               .filter(F.col('status')!='not_received')\
               .select('document', F.to_date(F.col('sentAt')[0:10], 'yyyy-M-d').alias('DT_INTERACAO'), 'type')\

dfinteractions = dfinteractions.withColumn('aux', F.lit(1)).groupby('document','DT_INTERACAO').pivot('type').sum('aux')

dfinteractions = dfinteractions.join(dfdeals, on = dfinteractions.document == dfdeals.CPF, how = 'left').drop(*['VARIAVEL_RESPOSTA_ACORDO','CPF'])

dfinteractions = dfinteractions.withColumn('ATIVACAO', F.when((F.col('DT_INTERACAO') <= F.col('DT_ACORDO'))\
                                                              | ((F.col('DT_INTERACAO') > F.col('DT_ACORDO')) & (F.col('status').isin(['expired','broken'])))\
                                                              | (F.col('DT_ACORDO').isNull())
                                                              , 'ATIVADO').otherwise('NAO_ATIVADO'))\
                 .orderBy(F.col('DT_ACORDO').desc()).dropDuplicates(['document','DT_INTERACAO',])\
                 .withColumn('email', (F.when(F.col('email').isNull(), 0).otherwise(F.col('email'))))\
                 .withColumn('sms', (F.when(F.col('sms').isNull(), 0).otherwise(F.col('sms'))))



dfValInt = dfinteractions.select('document',F.col('DT_INTERACAO').alias('PRIMEIRO_ACIONAMENTO')).orderBy('document',F.col('PRIMEIRO_ACIONAMENTO').asc()).dropDuplicates(['document'])

dfinteractions = dfinteractions.groupby('document')\
                .pivot('ATIVACAO').sum('email','sms')\
                .select(F.col('document').alias('CPF'), F.col('ATIVADO_sum(email)').alias('ACION_EMAIL_QTD_ATIVADO'),\
                        F.col('NAO_ATIVADO_sum(email)').alias('ACION_EMAIL_QTD_NAO_ATIVADO'), F.col('ATIVADO_sum(sms)').alias('ACION_SMS_QTD_ATIVADO'),\
                        F.col('NAO_ATIVADO_sum(sms)').alias('ACION_SMS_QTD_NAO_ATIVADO'))\
                .withColumn('ACION_EMAIL_QTD_ATIVADO', (F.when(F.col('ACION_EMAIL_QTD_ATIVADO').isNull(), 0).otherwise(F.col('ACION_EMAIL_QTD_ATIVADO'))))\
                .withColumn('ACION_EMAIL_QTD_NAO_ATIVADO', (F.when(F.col('ACION_EMAIL_QTD_NAO_ATIVADO').isNull(), 0).otherwise(F.col('ACION_EMAIL_QTD_NAO_ATIVADO'))))\
                .withColumn('ACION_SMS_QTD_ATIVADO', (F.when(F.col('ACION_SMS_QTD_ATIVADO').isNull(), 0).otherwise(F.col('ACION_SMS_QTD_ATIVADO'))))\
                .withColumn('ACION_SMS_QTD_NAO_ATIVADO', (F.when(F.col('ACION_SMS_QTD_NAO_ATIVADO').isNull(), 0).otherwise(F.col('ACION_SMS_QTD_NAO_ATIVADO'))))


display(dfinteractions)

# COMMAND ----------

#Adicionando coluna de primeiro acionamento
dfArq = dfArq.join(dfValInt, dfArq.CPF == dfValInt.document, how='left').drop('document')

#Adicinando as quantidades no DF principal
#dfArq = dfArq.join(dfinteractions, on ='CPF', how='left').drop('CPF').drop(*['dealID','_id','status'])
dfArq = dfArq.join(dfinteractions, on ='CPF', how='left').drop(*['dealID','_id','status'])

# COMMAND ----------

display(dfArq)

# COMMAND ----------

dfArq.coalesce(1).write.option('sep', ';').option('header', 'True').csv(os.path.join(caminho_trusted, 'full_temp'))
    
for file in dbutils.fs.ls(os.path.join(caminho_trusted, 'full_temp')):
  if file.name.split('.')[-1] == 'csv':
    dbutils.fs.cp(file.path, os.path.join(caminho_trusted,file_name))
  else:
    dbutils.fs.rm(file.path, True)
dbutils.fs.rm(os.path.join(caminho_trusted, 'full_temp'), True)
