# Databricks notebook source
import datetime
ArqDate = datetime.datetime.today() - datetime.timedelta(days=1)
date = datetime.datetime.today()
ArqDate = str(ArqDate.year)+'-'+str(ArqDate.month).zfill(2)+'-'+str(ArqDate.day).zfill(2)
date = str(date.year)+'-'+str(date.month).zfill(2)+'-'+str(date.day).zfill(2)

#date = '2022-05-24'
#ArqDate = '2022-05-23'
date, ArqDate

# COMMAND ----------

# MAGIC %run "/Shared/common-notebooks/dataprep-funcoes/initial-func"

# COMMAND ----------

# DBTITLE 1,Ajustando os diretórios
##Diretorios
caminho_base = '/mnt/qq-integrator/etl/santander/processed'
caminho_base_dbfs = '/dbfs/mnt/qq-integrator/etl/santander/processed'
caminho_trusted = '/mnt/ml-prd/ml-data/propensaodeal/santander/trusted_PJ'
caminho_trusted_dbfs = '/dbfs/mnt/ml-prd/ml-data/propensaodeal/santander/trusted_PJ'
caminho_sample = '/mnt/ml-prd/ml-data/propensaodeal/santander/sample'
caminho_base_models = '/mnt/ml-prd/ml-data/base_models'

##Selecionando arquivo
list_base = os.listdir(caminho_trusted_dbfs)
file_name = max(list_base)

fileDate = (file_name.split('.')[0].split('_')[5])[0:10]
fileDate = fileDate.replace('-','')

file_name, fileDate, ArqDate

# COMMAND ----------

df = spark.read.options(header = True, delimiter = ';').csv(os.path.join(caminho_trusted,file_name)).orderBy(F.col('ID_DEVEDOR').asc(),F.col('Contrato_Altair').asc())
df.count()
#display(df)

# COMMAND ----------

# DBTITLE 1,Info
dfInfo = spark.read.format('delta').load(os.path.join(caminho_base_models, 'Info',ArqDate)).filter(F.col('documentType') == 'cnpj')

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
dfDebts = spark.read.format('delta').load(os.path.join(caminho_base_models, 'Debts/CNPJ',ArqDate))

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
dfDebtsCre = dfDebts.filter(F.col('creditor') == 'santander').orderBy(F.col('_id').asc())
dfValCredor =  dfDebtsCre.select('_id','contract')

##Criando DF de divida nos demais credores
dfDebtsOut = dfDebts.filter(F.col('creditor') != 'santander').select('_id','creditor').orderBy(F.col('_id').asc())
dfDebtsOut = dfDebtsOut.dropDuplicates(['_id'])

#Join dos DFs de Info e Debts do credor
dfBase = dfInfo.join(dfDebtsCre.drop('document'), on='_id').orderBy(F.col('document').asc(),F.col('contract').asc())

##Join do arquivo ETL com os dados do mongo
dfArq = df.join(dfBase, (df.Contrato_Altair == dfBase.contract) & (df.ID_DEVEDOR == dfBase.document), 'left')
dfArq = dfArq.drop('creditor').drop('document').drop('originalAmoun').drop('contract')

#Join do DF completo com o DF de divida em outros credores e criando FLAG
dfArq = dfArq.join(dfDebtsOut, on= '_id', how='left')
dfArq = dfArq.withColumn('Divida_Outro_Credor', F.when(F.col('creditor').isNull(),False).otherwise(True)).drop('creditor')

##Criando Flag de outras dividas no mesmo credor
dfValCredor = dfValCredor.join(dfArq, (dfValCredor.contract == dfArq.Contrato_Altair) & (dfValCredor._id == dfArq._id ), 'left').select(dfValCredor._id,'contract','Contrato_Altair').filter(F.col('Contrato_Altair').isNull()).drop('Contrato_Altair').orderBy(F.col('_id').asc())
dfArq = dfArq.join(dfValCredor, on='_id', how='left')
dfArq = dfArq.withColumn('Divida_Mesmo_Credor', F.when(F.col('contract').isNull(),False).otherwise(True)).drop(*['contract','Contrato_Altair'])

dfArq = dfArq.distinct()


# COMMAND ----------

dfArq.count()

# COMMAND ----------

# DBTITLE 1,Simulação
DATA_Simu = spark.read.format('delta').load(os.path.join(caminho_base_models, 'Simulation',ArqDate))

# COMMAND ----------

dfSimu = DATA_Simu.select('personID','creditor','createdAt')\
        .filter(F.col('creditor')=='santander')\
        .filter(F.col('createdAt') <= (F.to_date(F.lit(fileDate), 'yyyyMMdd'))).orderBy(F.col('createdAt').desc())

##Contagem de simulações por CPF
dfSimuQtd = dfSimu.groupBy('personID').count()\
            .select(F.col('personID').alias('_id'), F.col('count').alias('QTD_SIMU')).orderBy(F.col('_id').asc())

#display(dfSimu)

# COMMAND ----------

##Criando coluna de quantidade de simulações por CPF no DF principal
dfArq = dfArq.join(dfSimuQtd, on= '_id', how = 'left')\
        .withColumn('QTD_SIMU', (F.when(F.col('QTD_SIMU').isNull(), 0).otherwise(F.col('QTD_SIMU'))))

#display(dfArq.orderBy(F.col('QTD_SIMU').desc()))

# COMMAND ----------

# DBTITLE 1,Autenticação
DATA_Auth = spark.read.format('delta').load(os.path.join(caminho_base_models, 'Authentication',ArqDate))

# COMMAND ----------

##Filtrando colunas de autenticações
dfAuth = DATA_Auth.select('tokenData.document','tokenData.contact','tokenData.contract','tokenData.creditor','createdAt')\
        .filter(F.col('creditor')=='santander')\
        .filter(F.col('createdAt') <= (F.to_date(F.lit(fileDate), 'yyyyMMdd'))).orderBy(F.col('createdAt').desc())


##Contagem de autenticação por CPF
dfAuthQtd = dfAuth.groupBy('document').count()\
            .select(F.col('document').alias('ID_DEVEDOR'), F.col('count').alias('QTD_AUTH')).orderBy(F.col('ID_DEVEDOR').asc())



#display(dfAuth)

# COMMAND ----------

dfArq = dfArq.orderBy(F.col('ID_DEVEDOR').asc())

##Criando coluna de quantidade de autenticações por CPF no DF principal
dfArq = dfArq.join(dfAuthQtd, on= 'ID_DEVEDOR', how = 'left')\
        .withColumn('QTD_AUTH', (F.when(F.col('QTD_AUTH').isNull(), 0).otherwise(F.col('QTD_AUTH'))))


##Criando coluna de quantidade de autenticações por CPF e Contrato no DF principal --Não disponivel para Credsystem
#dfArq = dfArq.join(dfAuthQtdContract, on= 'CPF', how = 'left')\
#            .withColumn('QTD_AUTH_CONTRATO', (F.when(F.col('QTD_AUTH_CONTRATO').isNull(), 0).otherwise(F.col('QTD_AUTH_CONTRATO'))))

# COMMAND ----------

display(dfArq)

# COMMAND ----------

# DBTITLE 1,Interações
caminho_interacoes = '/mnt/bi-reports/engenharia_de_dados/acionamentos_santander_sms_pj_enriquecida'

dfInt = spark.read.option('sep', ';').option('header', 'True').parquet(caminho_interacoes)\
        .withColumn('concat_ws', F.concat_ws(';', 'campaign_name'))\
        .select('document','interactionDate','type','channel','concat_ws')

#DF para filtrar apenas documentos com interações e primeiro acionamento
dfValInt = dfInt.select('document',F.col('interactionDate').alias('PRIMEIRO_ACIONAMENTO')).orderBy('document',F.col('PRIMEIRO_ACIONAMENTO').asc()).dropDuplicates(['document'])

#DF para pegar quantidade de ativação
dfQtdAtiv = dfInt.withColumn('SMS_ATIVACAO', F.when((~(F.col('concat_ws').like('%NOVO%')) & (F.col('type') == 'sms')),1).otherwise(0))\
                 .withColumn('SMS_NAO_ATIVACAO', F.when((F.col('concat_ws').like('%NOVO%') & (F.col('type') == 'sms')),1).otherwise(0))\
                 .withColumn('VOICE_ATIVACAO', F.when((~(F.col('concat_ws').like('%NOVO%')) & (F.col('type') == 'call')),1).otherwise(0))\
                 .withColumn('VOICE_NAO_ATIVACAO', F.when((F.col('concat_ws').like('%NOVO%') & (F.col('type') == 'call')),1).otherwise(0))\
                 .withColumn('EMAIL_ATIVACAO', F.when((~(F.col('concat_ws').like('%NOVO%')) & (F.col('type') == 'email')),1).otherwise(0))\
                 .withColumn('EMAIL_NAO_ATIVACAO', F.when((F.col('concat_ws').like('%NOVO%') & (F.col('type') == 'email')),1).otherwise(0)).drop('concat_ws')\
                 .groupBy('document').agg(F.sum(F.col('SMS_ATIVACAO')).alias('ACION_SMS_QTD_ATIVADO'),F.sum(F.col('SMS_NAO_ATIVACAO')).alias('ACION_SMS_QTD_NAO_ATIVADO'),\
                                          F.sum(F.col('VOICE_ATIVACAO')).alias('ACION_VOICE_QTD_ATIVADO'),F.sum(F.col('VOICE_NAO_ATIVACAO')).alias('ACION_VOICE_QTD_NAO_ATIVADO'),\
                                          F.sum(F.col('EMAIL_ATIVACAO')).alias('ACION_EMAIL_QTD_ATIVADO'),F.sum(F.col('EMAIL_NAO_ATIVACAO')).alias('EMAIL_VOICE_QTD_NAO_ATIVADO'))

# COMMAND ----------

#Adicionando coluna de primeiro acionamento
dfArq = dfArq.join(dfValInt, dfArq.ID_DEVEDOR == dfValInt.document, how='left').drop('document')

#Adicinando as quantidades no DF principal
dfArq = dfArq.join(dfQtdAtiv, dfArq.ID_DEVEDOR == dfQtdAtiv.document, how='left').drop('document')


dfArq = dfArq.withColumn('DATA_ENTRADA_DIVIDA',F.to_date('DATA_ENTRADA_DIVIDA','dd/MM/yyyy'))\
             .withColumn('PRIMEIRO_ACIONAMENTO',F.to_date('PRIMEIRO_ACIONAMENTO','yyyy-MM-dd'))

# COMMAND ----------

# DBTITLE 1,Acordos
"""#Buscando todos acordos do credor
dfRespostaAcordo = variavel_resposta_acordo_atualizado('santander', datetime(2020,1,1), datetime(2022,4,28), documentType='cnpj')
dfRespostaAcordo = spark.createDataFrame(dfRespostaAcordo)\
                  .select(F.col('document').alias('ID_DEVEDOR'),F.col('contract').alias('Contrato_Altair'),F.col('varName').alias('VARIAVEL_RESPOSTA'), F.col('createdAt').alias('DT_ACORDO'))\
                  .orderBy(F.col('ID_DEVEDOR').asc())

#Criando variavel resposta de acordo
dfArq = dfArq.join(dfRespostaAcordo, on=(['ID_DEVEDOR']), how= 'left').drop(dfRespostaAcordo.Contrato_Altair)
dfArq = dfArq.withColumn('VARIAVEL_RESPOSTA', (F.when(F.col('VARIAVEL_RESPOSTA').isNull(), False).otherwise(F.col('VARIAVEL_RESPOSTA'))))"""

# COMMAND ----------

# DBTITLE 1,Pagamentos
"""#Recuperando contratos da deals
dfDeals = spark.read.schema(SCHEMA).json('dbfs:/mnt/bi-reports/export_full/person_without_project/20220429/')\
.select('_id',  F.col('deals.creditor').alias('creditor'), F.col('deals._id').alias('dealID'),(F.col('deals.offer.debts')).alias('debts'))\
.withColumn('deals',F.arrays_zip('dealID','debts','creditor'))\
.select('_id', F.explode('deals').alias('deals'))\
.select('_id', F.col('deals.dealID').alias('dealID'), F.explode(F.col('deals.debts')).alias('debts'), F.col('deals.creditor').alias('creditor'))\
.filter((F.col('creditor') == 'santander'))\
.select('_id', 'dealID', 'debts.contract')\


#Buscando todos pagamentos do credor
dfRespostaPagamento = variavel_resposta_pagamento('santander', datetime(2020,1,1), datetime(2022,4,28))
dfRespostaPagamento = spark.createDataFrame(dfRespostaPagamento)

#Recuperando codigo do contrato
dfRespostaPagamento = dfRespostaPagamento.join(dfDeals, on=('dealID'), how= 'inner').drop('Pagto_Parcela1').drop('Pagto_Demais_Parcelas').drop('dealID').drop('_id').orderBy(F.col('document').asc())
#display(dfRespostaPagamento)

#Criando variavel resposta de pagamento
dfArq = dfArq.join(dfRespostaPagamento, (dfArq.ID_DEVEDOR == dfRespostaPagamento.document), how= 'left').drop('document').drop('contract')
dfArq = dfArq.withColumn('PAGTO_A_VISTA', (F.when(F.col('PAGTO_A_VISTA').isNull(), False).otherwise(F.col('PAGTO_A_VISTA'))))\
.withColumn('PAGTO', (F.when(F.col('PAGTO').isNull(), False).otherwise(F.col('PAGTO'))))\
.withColumn('Qtd_parcelas', (F.when(F.col('Qtd_parcelas').isNull(), 0).otherwise(F.col('Qtd_parcelas'))))

dfArq = dfArq.distinct()
#display(dfArq)"""

# COMMAND ----------

# DBTITLE 1,Salvando no blob
dfArq.coalesce(1).write.option('sep', ';').option('header', 'True').csv(os.path.join(caminho_trusted, 'full_temp'))

for file in dbutils.fs.ls(os.path.join(caminho_trusted, 'full_temp')):
  if file.name.split('.')[-1] == 'csv':
    dbutils.fs.cp(file.path, os.path.join(caminho_trusted,file_name))
  else:
    dbutils.fs.rm(file.path, True)
dbutils.fs.rm(os.path.join(caminho_trusted, 'full_temp'), True)
