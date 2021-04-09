from pyspark.sql import *
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.functions  import date_format

#Extreacting data from Azure Blob Storage.
             
dbutils.fs.mount(
  source = "wasbs://<container-name>@<storage-account-name>.blob.core.windows.net",
  mount_point = "/mnt/<mount-name>",
  extra_configs = {"<conf-key>":dbutils.secrets.get(scope = "<scope-name>", key = "<key-name>")})

#Tranformations,

#date format
def dateformat(df,format):
  try:
    for i in range(0,len(df.dtypes)):
      if (df.dtypes[i][1] == 'date'):
        column = df.dtypes[i][0]
        df = df.withColumn(column,F.date_format(F.col(column),format) )
    return df
  except Exception as dateformaterror:
    raise dateformaterror

#Load into database
jdbcHostname = "azsqlshackserver.database.windows.net"
jdbcPort = "1433"
jdbcDatabase = "azsqlshackdb"
properties = {
 "user" : "akhil_vallala",
 "password" : "******" }

url = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname,jdbcPort,jdbcDatabase)
mydf = sqlContext.read.csv("/FileStore/tables/1000_Sales_Records-d540d.csv",header=True)

from pyspark.sql import *
import pandas as pd
myfinaldf = DataFrameWriter(mydf)
myfinaldf.jdbc(url=url, table= "TotalProfit", mode ="overwrite", properties = properties)
