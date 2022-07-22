# Databricks notebook source
# MAGIC %run "/Shared/common-notebooks/dataprep-funcoes/initial-func"

# COMMAND ----------

import pandas as pd
import pickle
import numpy as np
import os

from sklearn import metrics

import matplotlib.pyplot as plt
from matplotlib import pyplot

# COMMAND ----------

# Seleção de Modelo e DF
filename = '/dbfs/mnt/ml-prd/ml-data/propensaodeal/pag/models/deals_model_v1.sav' # Caminho Modelo

#Caminho da base de dados
caminho_base = "/mnt/ml-prd/ml-data/propensaodeal/pag/trusted/"
caminho_base_dbfs = "/dbfs/mnt/ml-prd/ml-data/propensaodeal/pag/trusted/"
list_base = os.listdir(caminho_base_dbfs)

#Nome da Base de Dados
file = max(list_base)

#Separador
separador_ = ";"

#Decimal
decimal_ = "."

outputpath = 'mnt/ml-prd/ml-data/propensaodeal/pag/output/'
outputpath_dbfs = '/dbfs/mnt/ml-prd/ml-data/propensaodeal/pag/output/'

file

# COMMAND ----------

df = pd.read_csv(caminho_base_dbfs+file, sep = separador_, decimal = decimal_)

# COMMAND ----------

# As variáveis serão separadas em alguns tipos:

## Identificação (Ex: Nome, CPF, RG...)
var_id = ['CPF']

## Numéricas
var_num = ['DIAS_ATRASO','VALOR_ULTIMA_FATURA','QTD_AUTH','QTD_SIMU','ACION_EMAIL_QTD_ATIVADO','ACION_EMAIL_QTD_NAO_ATIVADO','ACION_SMS_QTD_ATIVADO','ACION_SMS_QTD_NAO_ATIVADO']

## Categóricas
var_cat = ['UF1','UF2','UF3']
## Datas
var_dt = ['DATA_DIVIDA','DATA_ULTIMA_FATURA','PRIMEIRO_ACIONAMENTO']

## Binárias
var_telefone = [col for col in df if col.startswith('Telefone')]
var_email = [col for col in df if col.startswith('Email')]
var_skip = [col for col in df if col.startswith('Skip')]
var_divida = [col for col in df if col.startswith('Divida')]

# COMMAND ----------

df_modelo = df.copy()
df_score = pd.DataFrame()
modelo=pickle.load(open(filename, 'rb'))
var_relevantes = modelo.get_booster().feature_names

# COMMAND ----------

#### Identificadora
df_score[var_id] = df_modelo[var_id]

#### Variáveis Numéricas
for v1 in var_num:
  if v1 in var_relevantes:
    try:
      x = df_modelo[v1].str.replace(',', '.',1)
    except:
      x = df_modelo[v1]
    x = pd.to_numeric(x, errors='coerce')
    df_score[v1] = x
    
    
#### Variáveis Categóricas
df_cat = pd.get_dummies(df_modelo[var_cat])
col_cat = df_cat.columns
df_cat_temp = pd.DataFrame()
for v2 in col_cat:
  if v2 in var_relevantes:
    df_cat_temp[v2] = df_cat[v2]

df_score = pd.concat([df_score,df_cat_temp], axis=1)

#### Variáveis Temporais
df_data=pd.DataFrame()
for col in var_dt:
  
  df_data[col] = pd.to_datetime(df_modelo[col].str[:10],infer_datetime_format=True)
  #df_data[col] = df_modelo[col]
  ano_col = pd.DatetimeIndex(df_data[col]).year
  mes_col = pd.DatetimeIndex(df_data[col]).month
  dia_col = pd.DatetimeIndex(df_data[col]).day
  
  df_data['ano_'+col] = ano_col
  df_data['mes_'+col] = mes_col
  df_data['dia_'+col] = dia_col
  
  df_data['delta_'+col] = ((pd.to_datetime("now") - pd.DatetimeIndex(df_data[col])).days)/365.25
  df_data = df_data.drop(columns = col)


col_dt = df_data.columns
df_data_temp = pd.DataFrame()
for v3 in col_dt:
  if v3 in var_relevantes:
    df_data_temp[v3] = df_data[v3]
    
df_score = pd.concat([df_score,df_data_temp], axis=1)

#### Variáveis Binárias:
df_bin = pd.concat([df_modelo[var_telefone],df_modelo[var_skip], df_modelo[var_divida]], axis=1)
col_bin = df_bin.columns
df_bin_temp = pd.DataFrame()
for v4 in col_bin:
  if v4 in var_relevantes:
    df_bin_temp[v4] = df_bin[v4]

df_score = pd.concat([df_score,df_bin_temp], axis=1)

# Rank:
#df_rank = pd.get_dummies(df_modelo[var_rank],drop_first = True)
#col_rank = df_rank.columns
#df_rank_temp = pd.DataFrame()
#for v5 in col_rank:
#  if v5 in var_relevantes:
#    df_rank_temp[v5] = df_rank[v5]

#df_score = pd.concat([df_score,df_rank_temp], axis=1)

df_email = pd.get_dummies(df_modelo[var_email],drop_first = True)
col_email = df_email.columns
df_email_temp = pd.DataFrame()
for v6 in col_email:
  if v6 in var_relevantes:
    df_email_temp[v6] = df_email[v6]
    
df_score = pd.concat([df_score,df_email_temp], axis=1)

df_score.replace({False: 0, True: 1}, inplace=True)

df_score = df_score.drop_duplicates()

# COMMAND ----------

df_total = pd.DataFrame()
for j in var_relevantes:
  try:
    x = df_score[j]
    df_total[j] = x
  except:
    x = np.nan
    df_total[j] = x

# COMMAND ----------

chave = df_score['CPF'].astype(str)
p_1 = modelo.predict_proba(df_total)[:,1]
dt_gh = pd.DataFrame({'Chave': chave, 'P_1':p_1})

# COMMAND ----------

dt_gh['GH'] = np.where(dt_gh['P_1'] <= 0.15462458, 0,
                                np.where(np.bitwise_and(dt_gh['P_1'] > 0.15462458, dt_gh['P_1'] <= 0.18387774), 1,
                                         np.where(np.bitwise_and(dt_gh['P_1'] > 0.18387774, dt_gh['P_1'] <= 0.5029465), 2,
                                                  np.where(np.bitwise_and(dt_gh['P_1'] > 0.5029465, dt_gh['P_1'] <= 0.63928771), 3,
                                                           np.where(np.bitwise_and(dt_gh['P_1'] > 0.63928771, dt_gh['P_1'] <= 0.72260146), 4,
                                                                     np.where(dt_gh['P_1'] > 0.72260146,5,0))))))

# COMMAND ----------

dt_gh[['GH']].value_counts()

# COMMAND ----------

try:
  dbutils.fs.rm(outputpath, True)
except:
  pass
dbutils.fs.mkdirs(outputpath)

dt_gh.to_csv(open(os.path.join(outputpath_dbfs, 'pre_output:'+file),'wb'))
os.path.join(outputpath_dbfs, 'pre_output:'+file)

# COMMAND ----------


