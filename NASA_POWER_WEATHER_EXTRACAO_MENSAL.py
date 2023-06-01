# Databricks notebook source
# MAGIC %md 
# MAGIC # Extração Mensal

# COMMAND ----------

from datetime import datetime, timedelta
import requests

# COMMAND ----------

# Parametros iniciais de Extração da API
# Obter data atual
current_date = datetime.now()

# date_str = "20230501"
# current_date = datetime.strptime(date_str, "%Y%m%d")

# Calcula o início do mês anterior
previous_month = current_date.replace(day=1) - timedelta(days=1)
year_month = previous_month.strftime("%Y%m")
year = previous_month.strftime("%Y")
begin_previous_month = previous_month.replace(day=1).strftime("%Y%m%d")

# Calcula o fim do mês anterior
end_previous_month = previous_month.strftime("%Y%m%d")

variables = "ALLSKY_SFC_SW_DWN,CLRSKY_SFC_SW_DWN,ALLSKY_KT,ALLSKY_SFC_LW_DWN,ALLSKY_SFC_PAR_TOT,CLRSKY_SFC_PAR_TOT,ALLSKY_SFC_UVA,ALLSKY_SFC_UVB,ALLSKY_SFC_UV_INDEX,T2M,T2MDEW,T2MWET,TS,T2M_RANGE,T2M_MAX,T2M_MIN"

community = "RE"

lat = "-23.5515"
long = "-46.6392" 

format_data = "CSV"

# COMMAND ----------

start_date = begin_previous_month
end_date = end_previous_month
print("Data Início: {}, Data Fim {}".format(start_date, end_date))
url = "https://power.larc.nasa.gov/api/temporal/daily/point?parameters=" + variables + "&community=" + community + "&longitude=" + long + "&latitude=" + lat + "&start=" + start_date + "&end=" + end_date + "&format=" + format_data
response = requests.get(url)
data = response.text

if response.status_code == 200:
    file_path = "/mnt/projeto_climatico/landing/nasa_solar_flux_temperature/" + str(year) + "/nasa_solar_flux_temperature_" + str(year_month) + ".csv"
    dbutils.fs.rm(file_path)
    dbutils.fs.put(file_path, data)

    print("Arquivo CSV {} salvo com sucesso.".format(file_path))
else:
    print("Erro na requisição:", response.status_code)
