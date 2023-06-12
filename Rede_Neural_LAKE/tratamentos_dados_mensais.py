#!/usr/bin/env python
# coding: utf-8

# In[23]:


# Programa que faz a leitura do arquivo historico_diario.csv na camada SILVER do Data Lake e 
# converte para dados médios mensais (1981 até 2022) salvando na camada GOLD para serem consumidos pelo 
# modelo de Machine Learning


# Nome do programa: historico_mensal.csv
# Dado de entrada: Arquivo historico_diario.csv com dados tratados diários da radiação solar entre 1981 e 2022
# Dados de saída: Arquivo historico_mensal.csv com dados tratados de média mensal da radiação solar entre 1985 e 2022


# Importação de bibliotecas
import pandas as pd

pd.set_option('display.max_columns', 50)

pd.set_option('display.max_rows', 600)


# In[24]:


# Endereço base de entrada do Data Lake
endereco_base_entrada = "SILVER/"

# Nome do arquivo que será lido com dados de entrada
nome_arquivo_entrada = f"historico_diario.csv"

#Diretório de leitura do arquivo
leitura_arquivo_entrada = endereco_base_entrada + nome_arquivo_entrada



# Endereço base de saída do Data Lake
endereco_base_saida = "GOLD/"

# Nome do arquivo que será escrito com dados de saída
nome_arquivo_saida = "historico_mensal.csv"

#Diretório de leitura do arquivo
leitura_arquivo_saida = endereco_base_saida + nome_arquivo_saida


# In[25]:


# Leitura de dados de entrada
df = pd.read_csv(leitura_arquivo_entrada, sep = ",")

df.head(300)


# In[26]:


#Tratamento de dados para converter dados -999.0 para 0.0
df = df.replace(-999.0, 0.0)


# In[27]:


#Cálculo da média mensal dos dados
df_2 = df.groupby(['YEAR', 'MO'])['ALLSKY_SFC_LW_DWN'].mean().reset_index()


# In[28]:


df_2.head(504)


# In[29]:


df_2['date'] = pd.to_datetime(df_2['YEAR'].astype(str) + '-' + df['MO'].astype(str) + '-1')
df_2['timestamp'] = df_2['date'].dt.strftime('%Y-%m-%d')


# In[30]:


df_2.head(10)


# In[31]:


# Reordenar as colunas
df_3 = df_2[['timestamp', 'ALLSKY_SFC_LW_DWN']]

df_3.head(50)


# In[32]:


# Converter a coluna "timestamp" para o tipo de dados datetime
df_3['timestamp'] = pd.to_datetime(df_3['timestamp'])

# Filtra as linhas com ano maior que 1985
df_filtered = df_3[df_3['timestamp'].dt.year > 1985]

df_filtered.head(10)


# In[33]:


# Salvando arquivo tratado na camada TRUSTED
df_filtered.to_csv(leitura_arquivo_saida, encoding = "utf-8-sig", sep = ";", index="False")

