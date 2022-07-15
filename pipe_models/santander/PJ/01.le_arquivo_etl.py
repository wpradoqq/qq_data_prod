# Databricks notebook source
import time
time.sleep(300)

# COMMAND ----------

# MAGIC %sh pwd

# COMMAND ----------

import datetime
date = datetime.datetime.today()
date = str(date.year)+'-'+str(date.month).zfill(2)+'-'+str(date.day).zfill(2)
date

# COMMAND ----------

# MAGIC %run "/Shared/common-notebooks/dataprep-funcoes/initial-func"

# COMMAND ----------

# MAGIC %run "/ml-prd/propensao-deal/santander/training/1-func-common"

# COMMAND ----------

# DBTITLE 1,Ajustando os diretórios
from itertools import chain
from datetime import datetime,timedelta

##Diretorios
caminho_base = '/mnt/qq-integrator/etl/santander/processed'
caminho_base_dbfs = '/dbfs/mnt/qq-integrator/etl/santander/processed'
caminho_trusted = '/mnt/ml-prd/ml-data/propensaodeal/santander/trusted_PJ'
caminho_trusted_dbfs = '/dbfs/mnt/ml-prd/ml-data/propensaodeal/santander/trusted_PJ'
caminho_sample = '/mnt/ml-prd/ml-data/propensaodeal/santander/sample'

##Selecionando arquivo
file_names = get_creditor_etl_file_dates('santander')
a_excluir = []
for file in file_names:
  if "OY_QUEROQUITAR_D" not in file:
    a_excluir.append(file)
for file in a_excluir:
  del file_names[file]
  
file_dates = []
for file in file_names:
  file_dates.append(file_names[file])
max_file_date = max(file_dates)

for file in file_names:
  valor = file_names[file]
  if valor == max_file_date:
    file_name = file
    break
file_name

# COMMAND ----------

from pyspark.sql.types import *
schema = getStructEtlField("temp_schema")

dfOne = spark.read.format("csv") \
                   .option("header", False) \
                   .option("delimiter", ";") \
                   .schema(schema) \
                   .load(os.path.join(caminho_base,file_name)) \
                   .limit(1)

dfOne = dfOne.withColumn("Tipo_Pessoa", F.ltrim(F.col("Tipo_Pessoa")))
dfOne = dfOne.withColumn("Tipo_Pessoa", F.rtrim(F.col("Tipo_Pessoa")))
firstOne = dfOne.select(F.to_date(F.col("Tipo_Pessoa"),"yyyy-MM-dd").alias("Data_Arquivo")).first()

df = spark.read.format("csv") \
               .option("header", False) \
               .option("delimiter", ";") \
               .schema(schema) \
               .load(os.path.join(caminho_base,file_name))
  
df = df.filter((F.col("Tipo_Registro").contains("0") == False) & (F.col("Tipo_Registro").contains("9") == False))
df = df.filter(df["Tipo_Pessoa"] == "J")
df = df.drop("Tipo_Registro")

df = df.withColumn("Nome_Arquivo", F.lit(file_name))
df = df.withColumn("Data_Arquivo", F.to_date(F.lit(firstOne.Data_Arquivo)))

df = df.select(F.col('Data_Arquivo'),F.col("Nome_Arquivo"),F.col('Tipo_Pessoa'),F.col('Numero_Documento'),F.col('Nome'),F.col('Endereco1'),F.col('Bairro1'),F.col('Cidade1'),F.col('Uf1'),F.col('Cep1'),F.col('Endereco2'),F.col('Bairro2'),F.col('Cidade2'),F.col('Uf2'),F.col('Cep2'),F.col('Endereco3'),F.col('Bairro3'),F.col('Cidade3'),F.col('Uf3'),F.col('Cep3'),F.col('Telefone1'),F.col('Telefone2'),F.col('Telefone3'),F.col('Telefone4'),F.col('Telefone5'),F.col('Telefone6'),F.col('Telefone7'),F.col('Telefone8'),F.col('Telefone9'),F.col('Telefone10'),F.col('Email'),F.col('Indicador_Correntista'),F.col('Contrato_Altair'),F.col('Numero_Operacao_LY'),F.col('Descricao_Produto'),F.col('Saldo_Devedor_Contrato'),F.col('Dias_Atraso'),F.col('Codigo_Politica'),F.col('Taxa_Padrao'),F.col('Desconto_Padrao'),F.col('Percentual_Min_Entrada'),F.col('Valor_Min_Parcela'),F.col('Qtde_Min_Parcelas'),F.col('Qtde_Max_Parcelas'),F.col('Qtde_Parcelas_Padrao'),F.col('Forma_Pgto_Padrao'),F.col('Total')) 

# COMMAND ----------

df = df.withColumn('Saldo_Devedor_Contrato', F.regexp_replace(F.split('Saldo_Devedor_Contrato','\+')[1],',','.').cast('float'))\
       .withColumn('Taxa_Padrao', F.regexp_replace(F.split('Taxa_Padrao','\+')[1],',','.').cast('float'))\
       .withColumn('Desconto_Padrao', F.regexp_replace(F.split('Desconto_Padrao','\+')[1],',','.').cast('float'))\
       .withColumn('Percentual_Min_Entrada', F.regexp_replace(F.split('Percentual_Min_Entrada','\+')[1],',','.').cast('float'))\
       .withColumn('Valor_Min_Parcela', F.regexp_replace(F.split('Valor_Min_Parcela','\+')[1],',','.').cast('float'))\
       .withColumn('Numero_Documento', F.regexp_replace(F.col('Numero_Documento'),'[.,\-,/]',''))\
       .withColumn('Dias_Atraso', F.col('Dias_Atraso').cast('int'))\
       .withColumn('Qtde_Min_Parcelas', F.col('Qtde_Min_Parcelas').cast('int'))\
       .withColumn('Qtde_Max_Parcelas', F.col('Qtde_Max_Parcelas').cast('int'))\
       .withColumn('Qtde_Parcelas_Padrao', F.col('Qtde_Parcelas_Padrao').cast('int'))\
       .withColumn('Contrato_Altair', F.col('Contrato_Altair')[15:12])

display(df)

# COMMAND ----------

tab_training = df.toPandas()
colunas_tab1=['Numero_Documento','Contrato_Altair','Indicador_Correntista','Cep1','Cep2','Cep3','Forma_Pgto_Padrao','Taxa_Padrao','Desconto_Padrao','Qtde_Min_Parcelas','Qtde_Max_Parcelas','Qtde_Parcelas_Padrao','Valor_Min_Parcela','Codigo_Politica','Saldo_Devedor_Contrato','Dias_Atraso','Descricao_Produto']
Tab_vals1=tab_training[colunas_tab1]

Tab_vals1.insert(0,'ID_Credor','std')
Tab_vals1['Indicador_Correntista']=Tab_vals1['Indicador_Correntista'].str.replace("N","0").str.replace("S","1").astype(int).astype(bool)

Tab_vals1=Tab_vals1.rename(columns={'Numero_Documento' : 'document'})

# COMMAND ----------

data_hj=datetime.strptime(str((datetime.today()-timedelta(hours=3)).strftime("%d/%m/%Y"))+' 03:00:00',"%d/%m/%Y %H:%M:%S")
param_date_time = "{0} {1}".format(date, '03:00:00')
data_ref=datetime.strptime(param_date_time,"%Y-%m-%d %H:%M:%S")

col_person = getPyMongoCollection('col_person')

####SELECIONANDO AS DEMAIS INFORMAÇÕES####
print("\nSelecionando as informações da plataforma QQ...")
partes=list(range(0,Tab_vals1.shape[0],10000))
if(partes[-1]!=Tab_vals1.shape[0]):
  partes.append(Tab_vals1.shape[0])
  
Tab_rest=[]
for i in range(0,len(partes)-1):
  print("\nExecutando a parte "+str(i+1)+" de "+str(len(partes)-1)+" da querys das informações...")  
  lista_CPFs=list(Tab_vals1['document'][partes[i]:(partes[i+1])])
  query=[
    {
        "$match" : {
            "document" : {"$in" : lista_CPFs}
        }
    },
    {
        "$project" : {
            "_id" : 1,
            "document" : 1,
            "genero" : {"$ifNull" : ["$info.gender",""]},
            "faixa_etaria" : {
                                "$floor": {
                                        "$divide": [{
                                            "$subtract": [data_hj,{"$ifNull" : ["$info.birthDate",data_hj]}]
                                        }, 31540000000]
                                    }
                         },
           "debts" : {
                        "$filter" : {
                            "input" : {"$ifNull" : ["$debts",[]]},
                            "cond" : {"$eq" : ["$$this.creditor","santander"]}
                        }
                    }
        }
      }
    ]
  Tab_rest.append(list(col_person.aggregate(pipeline=query,allowDiskUse=True)))
Tab_rest=pd.DataFrame(list(chain.from_iterable(Tab_rest)))

# COMMAND ----------

Tab_vals1=Tab_vals1.rename(columns={'_id' : 'ID_DEVEDOR'},inplace=False)

####DEBTS###
print('\nInformações da debts...')
Tab_debts=Tab_rest[['document','debts']].copy().explode(column=['debts'])
aux=pd.json_normalize(Tab_debts['debts'])
Tab_debts=Tab_debts.reset_index(drop=True)
Tab_debts=pd.concat([Tab_debts['document'],aux],axis=1)
Tab_debts=Tab_debts[['document','contract','createdAt','tags']]

###Formatando###
Tab_debts['createdAt']=Tab_debts['createdAt'].dt.strftime('%d/%m/%Y')
isna = Tab_debts['tags'].isna()
Tab_debts.loc[isna,'tags']=pd.Series([['sem_skip']] * isna.sum()).values
tags_values=['rank:a','rank:b','rank:c','rank:d','rank:e','sem_skip']
Tab_debts['tags']=Tab_debts['tags'].map(lambda x : list(set(x).intersection(tags_values))[0] if  len(list(set(x).intersection(tags_values)))>0 else 'sem_skip')
Tab_debts=pd.concat([Tab_debts,pd.get_dummies(Tab_debts['tags'],columns=tags_values).T.reindex(tags_values).T.fillna(0).astype(bool)],axis=1)
Tab_debts=Tab_debts.drop(columns=['tags'])
Tab_debts['chave']=Tab_debts['document']+":"+Tab_debts['contract']
Tab_debts=Tab_debts.drop(columns=['document','contract'])

###Concatenando###
Tab_vals1['chave']=Tab_vals1['document']+":"+Tab_vals1['Contrato_Altair']
Tab_vals1=pd.merge(Tab_vals1,Tab_debts,how='left',on='chave')
Tab_vals1=Tab_vals1.rename(columns={'createdAt' : 'DATA_ENTRADA_DIVIDA'},inplace=False)
  
#####FORMATANDO A TABELA PRINCIPAL E DROPANDO AS COLUNAS DESNECESSÁRIAS####
print('\nFormatando as tabelas principais e dropando as colunas desnecessárias...')

Tab_vals1.insert(2,'data_referencia',data_ref.strftime("%d/%m/%Y"))
columns_vals=['ID_Credor','document','Contrato_Altair','data_referencia','DATA_ENTRADA_DIVIDA','Indicador_Correntista','Cep1','Cep2','Cep3','Forma_Pgto_Padrao','Taxa_Padrao','Desconto_Padrao','Qtde_Min_Parcelas','Qtde_Max_Parcelas','Qtde_Parcelas_Padrao','Valor_Min_Parcela','Codigo_Politica','Saldo_Devedor_Contrato','Dias_Atraso','Descricao_Produto','rank:a','rank:b','rank:c','rank:d','rank:e','sem_skip']
Tab_vals1=Tab_vals1[columns_vals]
Tab_vals1=Tab_vals1.rename(columns={'document' : 'ID_DEVEDOR'},inplace=False)

# COMMAND ----------

Tab_vals1.to_csv(caminho_trusted_dbfs+'/Base_Santander_PJ_QQ_Model_'+date+'.csv',index=False,sep=";")
