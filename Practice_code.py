#Fetching data from Azure Blob Storage.
             
dbutils.fs.mount(
  source = "wasbs://<container-name>@<storage-account-name>.blob.core.windows.net",
  mount_point = "/mnt/<mount-name>",
  extra_configs = {"<conf-key>":dbutils.secrets.get(scope = "<scope-name>", key = "<key-name>")})


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
