# -*- coding: utf-8 -*-
"""previsor_energia_solar.ipynb

Automatically generated by Colaboratory.

"""

# Commented out IPython magic to ensure Python compatibility.
# Programa que faz a leitura do arquivo historico_mensal.csv na camada GOLD do Data Lake e 
# utliza dados médios mensais (1981 até 2022) para realizar a previsão de radiação com o modelo de redes neurais 

# Nome do programa: previsor_energia_solar
# Dado de entrada: Arquivo historico_mensal.csv com dados tratados de média mensal de radiação solar entre 1985 e 2022
# Dados de saída: Previsão da radiação solar entre 2015 e 2022


# Importação de bibliotecas
# %matplotlib inline

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt 

from sklearn.preprocessing import MinMaxScaler
from keras.layers import SimpleRNN
from keras.layers import LSTM
from keras.models import Sequential
from numpy.random import seed
from tensorflow.keras.utils import set_random_seed

# 
seed(42)
set_random_seed(42)

plt.rcParams['figure.figsize'] = 16,4

#Endereço base de leitura do Data Lake
#endereco_base = "/content/sample_data/"
endereco_base = "GOLD/"

# Nome do arquivo de entrada
nome_arquivo = "historico_mensal.csv"

# Endereço do arquivo de entrada
arquivo_leitura = endereco_base + nome_arquivo

# Leitura do arquivo de entrada
df = pd.read_csv(arquivo_leitura, sep=";")

df.head(10)

# Reordenar as colunas
df = df.reindex(columns=['timestamp', 'ALLSKY_SFC_LW_DWN'])

df.head(10)

# Definação da data de divisão entre os conjuntos de treinamento e teste
SPLIT_DATE = '2015-01-01'

# Definação do espaço da janela de tempo que será utilizado no previsor com redes neurais
WINDOW_SIZE = 7 # Janeja de 7 meses

# Divisão do dataframe dos conjuntos de treino e teste com base na variável SPLIT_DATE
df_train = df[df.timestamp < SPLIT_DATE].copy()
df_test = df[df.timestamp >= SPLIT_DATE].copy()

# Reescalar as variaveis de entrada da rede neural para ficarem entre -1 e 1
scaler = MinMaxScaler()
df_train['ALLSKY_SFC_LW_DWN'] = scaler.fit_transform(df_train['ALLSKY_SFC_LW_DWN'].values.reshape(-1, 1))
df_test['ALLSKY_SFC_LW_DWN'] = scaler.transform(df_test['ALLSKY_SFC_LW_DWN'].values.reshape(-1, 1))

def gen_rnn_inputs(df, window_size):
    X, y = [], []
    averages = df['ALLSKY_SFC_LW_DWN'].values
    
    for i in range(window_size, len(df)):
        X.append(averages[i-window_size: i])
        y.append(averages[i])
        
    return np.array(X), np.array(y)

# Divisão dos valores de X e y para os conjuntos de treino e teste com base na variável SPLIT_DATE e WINDOW_SIZE
X_train, y_train = gen_rnn_inputs(df_train, WINDOW_SIZE)      
X_test, y_test = gen_rnn_inputs(df_test, WINDOW_SIZE)

# Criação da estrutura do Modelo de Machine Learning com redes neurais
model = Sequential()
model.add(SimpleRNN(1, return_sequences=False, input_shape=(WINDOW_SIZE, 1)))

# Definação da função de erro
model.compile(optimizer='adam',loss='mean_squared_error')

# Apresenta o resumo do modelo de redes neurais que será utilizado
model.summary()

# Treinamento da rede neural
model.fit(np.expand_dims(X_train, axis=-1), y_train, epochs=100, batch_size=8, verbose=2)

# Previsão de dados do conjunto teste com base na rede treinada
y_pred = model.predict(np.expand_dims(X_test, axis=-1))

# Conversão para os valores originais das variveis reescaladas (-1 até 1)
df_test_preds = df_test.copy()
df_test_preds['ALLSKY_SFC_LW_DWN'] = np.zeros(WINDOW_SIZE).tolist() + scaler.inverse_transform(y_test.reshape(-1,1)).squeeze().tolist()
df_test_preds['Pred'] = np.zeros(WINDOW_SIZE).tolist() + scaler.inverse_transform(y_pred.reshape(-1,1)).squeeze().tolist()

# Plote dos dados da previsão da rede neural para o conjunto de teste
df_test_preds.plot(legend=True)

plt.xlabel('Meses')
plt.ylabel('Radiação horizontal (kWh/m2)')
plt.title('Radiação horizontal na cidade de São Paulo')

# Rotacionar os rótulos do eixo x em 45 graus
plt.xticks(rotation=90)

plt.show()

# Plotar o gráfico do conjunto de dados originais

df.plot(legend=True)
df = df.set_index('timestamp')
plt.xlabel('Meses')
plt.ylabel('Radiação horizontal (kWh/m2)')
plt.title('Radiação horizontal na cidade de São Paulo')

# Rotacionar os rótulos do eixo x em 45 graus
plt.xticks(rotation=90)

# Definir os ticks do eixo x a cada 12 dados
xticks = np.arange(0, len(df), 120)
plt.xticks(xticks)

plt.show()