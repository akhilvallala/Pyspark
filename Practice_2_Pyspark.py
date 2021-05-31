from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
spark = SparkSession.builder.appName("Pysparkdataframes").getOrCreate()

data = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]

schema = StructType([
    StructField("Firstname",StringType(),True),
    StructField("Middlename",StringType(),True),
    StructField("Lastname",StringType(),True),
    StructField("ID",StringType(),True),
    StructField("Gender",StringType(),True),
    StructField("Salary",IntegerType(),True)
])

df = spark.createDataFrame(data = data,schema = schema)

df.printSchema()

df.show(n = 10,truncate=False)

file_path = "./testdata/ fire-incidents/fire-incident.csv"
fire_df = spark.read.format("csv")\
            .option("header",True)\
            .option("inferSchema", True)\
            .load("./testdata/fire-incidents/fire-incidents.csv")

#Print columns in dataframe
fire_df.columns



#Work with JSON file
from pyspark.sql.types import ArrayType, BooleanType, FloatType, DateType, IntegerType

person_schema = StructType([
    StructField("id",IntegerType(),True),
    StructField("first_name",StringType(),True),
    StructField("last_name",StringType(),True),
    StructField("fav_movies",ArrayType(StringType()),True),
    StructField("salary",FloatType(),True),
    StructField("image_url",StringType(),True),
    StructField("date_of_birth",DateType(),True),
    StructField("active",BooleanType(),True)
])

file_path = "./testdata/persons.json"
person_df = spark.read.json(file_path,person_schema,multiLine = True)

# Col and Expr
from pyspark.sql.functions import col, expr

person_df.select("first_name","last_name","date_of_birth").show(5)

person_df.select(col("first_name"),col("last_name"),col("date_of_birth")).show(5)

person_df.select(expr("first_name"),expr("last_name"),expr("date_of_birth")).show(5)

from pyspark.sql.functions import concat_ws

person_df.select(concat_ws(' ',col("first_name"),col("last_name")).alias("full_name"),
                col("salary"),
                (col("salary")*0.10 + col("salary")).alias("Salary_Increment")).show(5)

person_df.select(concat_ws(' ',col("first_name"),col("last_name")).alias("full_name"),
                col("salary"),
                expr("salary * 0.10 + salary").alias("salary_increment")).show(5)
                
  
  #Filter and where
  person_df.filter("salary <= 3000").show(5)
  
  person_df.where(col("salary") <= 3000).show(5)
  
  person_df.where("salary <= 3000").show(5)
  
  person_df.where((col("salary") <= 3000) & (col("active")==True)).show(5)
  
  from pyspark.sql.functions import year
  
  person_df.filter((year("date_of_birth") == 2000) | (year("date_of_birth") == 1989)).show(5)
  
  from pyspark.sql.functions import array_contains
  
  person_df.where(array_contains(col("fav_movies"), "Land of the Lost")).show(5)
  
  #distinct drop duplicate orderby
  from pyspark.sql.functions import count,desc
  
  person_df.select("active").distinct().show()
  
  person_df.select(col("first_name"),
                year(col("date_of_birth")).alias("year"),
                col("active")).orderBy("year","first_name").show(10)
  
  drop_dup_df = person_df.select(col("first_name"),
                year(col("date_of_birth")).alias("year"),
                col("active")).dropDuplicates(["year","first_name"]).orderBy("year","first_name")
  
  person_df.select(col("first_name"),
                year(col("date_of_birth")).alias("year"),
                col("active")).orderBy("year",ascending = False).show(10)
  
  #Adding and droping columns
  
  aug_person_df1 = person_df.withColumn("Salary_Increment",expr("salary*0.10 + salary"))
  
  aug_person_df1.show(5)
  
  aug_person_df2 = aug_person_df1\
                    .withColumn("birth_year",year("date_of_birth"))\
                    .withColumnRenamed("fav_movies","movies")\
                    .withColumn("salary_x10",round(col("Salary_Increment"),2))\
                    .drop("Salary_Increment")
  
  #Working with bad data
  bad_movies_list = [Row(None, None, None),
                   Row(None, None, 2020),
                   Row("John Doe", "Awesome Movie", None),
                   Row(None, "Awesome Movie", 2021),
                   Row("Mary Jane", None, 2019),
                   Row("Vikter Duplaix", "Not another teen movie", 2001)]
  
  bad_movies_columns = ["actor_name","movie_title","produced_year"]
  
  bad_movie_df = spark.createDataFrame(bad_movies_list, schema=bad_movies_columns)
bad_movie_df.show()

#drop record if, any of the columns in dataset has null in it.
bad_movie_df.na.drop().show() 
bad_movie_df.na.drop("any").show() 

#drop only, if alll the records are null
bad_movie_df.na.drop("all").show() 

#Removes record, if only particular column has null values in it.
bad_movie_df.filter(col("actor_name").isNull() != True).show()

bad_movie_df.describe().show()

bad_movie_df.describe("produced_year").show()

#UDF

from pyspark.sql.functions import udf

def salarycassify(salary:float):
    if salary> 10000:
        salary_cassify = "Highest"
    elif salary>= 500:
        salary_cassify = "Medium"
    else:
        salary_cassify = "below avg"
    return salary_cassify

  
salarycassifyUDF = udf(salarycassify)

person_df.select("first_name","salary",salarycassifyUDF(col("salary")).alias("Rank")).show(5) 
  
  ##Challenge2

sales_df = spark.read.format('csv').option("header",True).schema(sales_schema).load("./testdata/salesdata/Sales_April_2019.csv")
# sales_df = spark.read.format('csv').option("header",True).schema(sales_schema).load("./testdata/salesdata")

#Remove Null values
sales_df = sales_df.na.drop("any")

sales_df.filter(col("Order ID").isNull() == True).show()

sales_df.filter(col("Order ID")=="Order ID").show(10)

sales_df_without_orderid = sales_df.filter(col("Order ID")!="Order ID")

#Extracting city and state address from store address
from pyspark.sql.functions import split

sales_df_without_columnnames.select("Purchase Address").show(10,False)

sales_df_without_columnnames.select("Purchase Address",split(col("Purchase Address"),",")).show(10,False)

sales_df_without_columnnames.select("Purchase Address",split(col("Purchase Address"),",").getItem(1)).show(10,False)

sales_df_without_columnnames.select("Purchase Address",split(col("Purchase Address"),",").getItem(2)).show(10,False)

sales_df_without_columnnames.select("Purchase Address",split(split(col("Purchase Address"),",").getItem(2), " ").getItem(1)).show(10,False)

sales_df_without_columnnames = sales_df_without_columnnames.withColumn("City",split(col("Purchase Address"),",").getItem(1))\
                                                            .withColumn("State",split(split(col("Purchase Address"),",").getItem(2), " ").getItem(1))

sales_df_without_columnnames.columns

# sales_df_with_changed_col_names = sales_df_without_columnnames.withColumnRenamed("Order ID","OrderID")\
#                             .withColumnRenamed("Quantity Ordered","Quantity")\
#                             .withColumnRenamed("Order Date","OrderDate")\
#                             .withColumnRenamed("Purchase Address","StoreAddress")
from pyspark.sql.types import StringType,IntegerType,FloatType
from pyspark.sql.functions import to_timestamp,year,month

sales_df_with_changed_col_names = sales_df_without_columnnames\
                            .withColumn("OrderID",col("Order ID").cast(IntegerType()))\
                            .withColumn("Quantity",col("Quantity Ordered").cast(IntegerType()))\
                            .withColumn("Price",col("Price Each").cast(FloatType()))\
                            .withColumn("OrderDate",to_timestamp(col("Order Date"),"MM/dd/yy HH:mm"))\
                            .withColumnRenamed("Purchase Address","StoreAddress")\
                            .drop("Order ID").drop("Quantity Ordered").drop("Price Each").drop("Order Date")
#Month and Year
sales_df_with_year_month = sales_df_with_changed_col_names.withColumn("ReportedYear",year(col("OrderDate")))\
                                                        .withColumn("Month",month(col("OrderDate")))

sales_df_final.write.mode("overwrite").partitionBy("ReportedYear","Month").parquet("./data/ouput")

#Aggregation
flights_df = spark.read.format('csv').option("header",True).option("inferSchema",True).load("./testdata/flight-summary.csv")

#Count and DistinctCount
from pyspark.sql.functions import count, countDistinct
flights_df.select(count("origin_airport"),count("dest_airport")).show()
flights_df.select(countDistinct("origin_airport"),countDistinct("dest_airport"),count("*")).show()

#Max, min, sum and avg
from pyspark.sql.functions import min,max,sum, sumDistinct, avg
flights_df.select(min("flight_count"),max("flight_count"),sum("flight_count"),avg("flight_count")).show()
flights_df.select(sumDistinct("flight_count")).show()

#Aggregation with group
flights_df.groupBy("origin_airport").count().orderBy("count",ascending = False).show(5,False)

(flights_df.groupBy("origin_airport")\
            .agg(max("flight_count").alias("max_flight_count"))\
            .orderBy("max_flight_count",ascending = False)).show(5)
flights_df.groupBy("origin_state","origin_city").count()\
        .where(col("origin_state")=="CA").orderBy("count",ascending = False).show()

(flights_df.groupBy("origin_airport")\
            .agg(max("flight_count"),
                 min("flight_count"),
                 sum("flight_count"),
                 count("flight_count"))).show(5,False)


#Reading from dbfs
sales_data_path = "dbfs:/FileStore/salesdata/raw"
sales_df = spark.read.format("csv").option("header",True).schema(schema).load(sales_data_path)
