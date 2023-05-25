# Databricks notebook source
import requests

# COMMAND ----------

# -BEGIN HEADER-
# NASA/POWER CERES/MERRA2 Native Resolution Daily Data 
# Dates (month/day/year): 01/01/2021 through 03/31/2021 
# Location: Latitude  -23.5515   Longitude -46.6392 
# Elevation from MERRA-2: Average for 0.5 x 0.625 degree lat/lon region = 790.19 meters
# The value for missing source data that cannot be computed or is outside of the sources availability range: -999 
# Parameter(s): 
# ALLSKY_SFC_SW_DWN       CERES SYN1deg All Sky Surface Shortwave Downward Irradiance (kW-hr/m^2/day) 
# CLRSKY_SFC_SW_DWN       CERES SYN1deg Clear Sky Surface Shortwave Downward Irradiance (kW-hr/m^2/day) 
# ALLSKY_KT               CERES SYN1deg All Sky Insolation Clearness Index (dimensionless) 
# ALLSKY_SFC_LW_DWN       CERES SYN1deg All Sky Surface Longwave Downward Irradiance (W/m^2) 
# ALLSKY_SFC_PAR_TOT      CERES SYN1deg All Sky Surface PAR Total (W/m^2) 
# CLRSKY_SFC_PAR_TOT      CERES SYN1deg Clear Sky Surface PAR Total (W/m^2) 
# ALLSKY_SFC_UVA          CERES SYN1deg All Sky Surface UVA Irradiance (W/m^2) 
# ALLSKY_SFC_UVB          CERES SYN1deg All Sky Surface UVB Irradiance (W/m^2) 
# ALLSKY_SFC_UV_INDEX     CERES SYN1deg All Sky Surface UV Index (dimensionless) 
# T2M                     MERRA-2 Temperature at 2 Meters (C) 
# T2MDEW                  MERRA-2 Dew/Frost Point at 2 Meters (C) 
# T2MWET                  MERRA-2 Wet Bulb Temperature at 2 Meters (C) 
# TS                      MERRA-2 Earth Skin Temperature (C) 
# T2M_RANGE               MERRA-2 Temperature at 2 Meters Range (C) 
# T2M_MAX                 MERRA-2 Temperature at 2 Meters Maximum (C) 
# T2M_MIN                 MERRA-2 Temperature at 2 Meters Minimum (C) 
# -END HEADER-

# COMMAND ----------

variables = "ALLSKY_SFC_SW_DWN,CLRSKY_SFC_SW_DWN,ALLSKY_KT,ALLSKY_SFC_LW_DWN,ALLSKY_SFC_PAR_TOT,CLRSKY_SFC_PAR_TOT,ALLSKY_SFC_UVA,ALLSKY_SFC_UVB,ALLSKY_SFC_UV_INDEX,T2M,T2MDEW,T2MWET,TS,T2M_RANGE,T2M_MAX,T2M_MIN"

community = "RE" 
lat = "-23.5515"
long = "-46.6392" 
start_date = "20000101"
end_date = "20230524"
format_data = "JSON"
# format_data = "CSV"

# COMMAND ----------

url = "https://power.larc.nasa.gov/api/temporal/daily/point?parameters=" + variables + "&community=" + community + "&longitude=" + long + "&latitude=" + lat + "&start=" + start_date + "&end=" + end_date + "&format=" + format_data

payload = {}
headers = {}

response = requests.request("GET", url, headers=headers, data=payload)

# COMMAND ----------

print(response.text)

# COMMAND ----------

df = spark.read.json(spark.sparkContext.parallelize([response.text]))

# COMMAND ----------

display(df)

# COMMAND ----------

from io import StringIO

csv_content = response.text

csv_string_io = StringIO(csv_content)

csv_rdd = spark.sparkContext.parallelize(csv_string_io)

df2 = spark.read.csv(csv_rdd, header=True, inferSchema=True)

# display(df_2)

# print(csv_string_io)

# COMMAND ----------

display(df2) 
