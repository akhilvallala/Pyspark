from pyspark.sql import *
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.functions  import date_format
import pandas as pd


#Extracting data from Azure Blob Storage.
             
dbutils.fs.mount(
  source = "wasbs://<container-name>@<storage-account-name>.blob.core.windows.net",
  mount_point = "/mnt/<mount-name>",
  extra_configs = {"<conf-key>":dbutils.secrets.get(scope = "<scope-name>", key = "<key-name>")})

#Cob=nverting data into dataframe
df = spark.read.text("/mnt/<mount-name>/...")

#Basic Tranformations

#Display duplicate records
def DisplayDuplicateRecords(dfWithDuplicates):
  try:
    dataframe = dfWithDuplicates.groupBy(dfWithDuplicates.columns).count().filter("count>1").drop('count')
    return dataframe
  except Exception as findduperecords:
    raise findduperecords
    
#Drop Dulpicate records
def DropDulicate(df):
  new_df = df.dropDuplicates()
  return new_df

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
    
#Remove Special char in dataframe    
def RemoveSpecialChar(df):
  try:
    for column in df.columns:
      new_df = df.withColumn(column, F.regexp_replace(F.col(column), "[^A-Za-z0-9_@!#%&*()+={}''<>,.""-:;~`/|\?]", ""));
    return new_df
  except Exception as specialcharerror:
    raise specialcharerror
    
def roundingoff(df,column_name,round_no):
  try:
    df = df.withColumn(column_name,F.round(F.col(column_name), round_no))
    return df
  except Exception as roundingofferror:
    raise roundingofferror
    
def datamismatchcolumns(dfsource,dftarget):
  try:
    list_col=[]
    count = 0
    cols=dfsource.columns
    for col in cols:
      list_col.append(dfsource.select(col).subtract(dftarget.select(col)))
    for  l in list_col :
      if l.count() > 0 :
        count+=1
        display(l)
    if count == 0:
      return True
    else:
      raise Exception
  except:
    raise Exception("There is a data mismatch between particular columns as mentioned above")
    
def datamismatchrecords(dfsource, dftarget):
  try:
    final_df = dfsource.subtract(dftarget)
    return final_df
  except:
    raise Exception("There is a data mismatch between two dataframes in particular records as mentioned above")
    
def nullvalidation(df,nonnullablecolumns):
  result = ""
  for column in nonnullablecolumns:
    non_acceptable_values_count = 0
    number_of_nulls_in_column = df.where(F.col(column).isNull()).count()
    number_of_nan_in_column = df.where(F.isnan(column)).count()
    non_acceptable_values_count += number_of_nulls_in_column + number_of_nan_in_column
    if non_acceptable_values_count == 0:
      pass
    else:
      result += "column "+column+" as null values in it: "+str(number_of_nulls_in_column)
      result += ","
      result += "column "+column+" as nan values in it: "+str(number_of_nan_in_column)
      result += "."
  return result
    
#Apply Transformation
def ApplyTransformation(df):
  df = DisplayDuplicateRecords(df)
  df = DropDulicate(df)
  df = dateformat(df,"yyyy MM dd")
  df = RemoveSpecialChar(df)
  return df
  
final_df = ApplyTransformation(df)
#Load into database
jdbcHostname = "azsqlshackserver.database.windows.net"
jdbcPort = "1433"
jdbcDatabase = "azsqlshackdb"
properties = {
 "user" : "akhil_vallala",
 "password" : "******" }

url = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname,jdbcPort,jdbcDatabase)
final_df.jdbc(url=url, table= "TotalProfit", mode ="overwrite", properties = properties)
