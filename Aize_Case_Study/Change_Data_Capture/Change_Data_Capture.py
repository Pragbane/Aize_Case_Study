# Databricks notebook source
# MAGIC %run "../Includes/Global_param_set"

# COMMAND ----------

#The first step is to get the date of the file
#display(dbutils.fs.ls("/mnt/aize_case_study/raw/Coordinates/20230924"))
filelist=dbutils.fs.ls(f"{mount_point}/{raw_folder_path}/{Module}/")
files_to_read = [file.name for file in list(filelist)]
for file in files_to_read:
    if file.startswith("coordinates"):
        filedate=file[12:20:1]

dbutils.fs.mv(f"{mount_point}/{raw_folder_path}/{Module}/coordinates_{filedate}.csv", f"{mount_point}/{raw_folder_path}/{Module}/{filedate}/{Source_File_Name}")   

# COMMAND ----------

###Geohash for geo code
from geohash2 import geohash
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F
from pyspark.sql.functions import split, lead, lag,coalesce, lit
###For partition
from pyspark.sql.window import Window

#Create dataframe for the source data
try:
    Coordinates_raw_df = spark.read.option("header", True).csv(f"{mount_point}/{raw_folder_path}/{Source_File_Name}")
    print("File read successfully")
except Exception as ex:
    print("Unable to read the file in mentioned mount")

### Raw data cleansing/ Source data validation starts here



#Step1: Check if there are any special delimiters, or if the data needs to be splitted between columns
try:
    split_columns=split(Coordinates_raw_df[0],';')
    Coordinates_processed_df=Coordinates_raw_df.withColumn("Longitude",split_columns.getItem(0)).withColumn("Latitude",split_columns.getItem(1))
    Coordinates_processed_df=Coordinates_processed_df.drop(Coordinates_processed_df.columns[0])
    print("File splitted successfully")
except Exception as ex:
    print("Unable to split")

#Step 2: Check if there are any null values in the data:

try:
    Longitude_null_count=Coordinates_processed_df.where(col("Longitude").isNull()).count()
    Latitude_null_count=Coordinates_processed_df.where(col("Longitude").isNull()).count()
    if (Longitude_null_count > 0 or Latitude_null_count >0):
        print("Null values found in one or many of the mandatory columns")
except Exception as ex:
    print("Null values found in mandatory columns")

#Step3: check if there are any duplicates in the data:
try:
    Coordinates_processed_df.groupby('Longitude','Latitude').count().where('count >1').show()
    duplicate_record_count=Coordinates_processed_df.groupby('Longitude','Latitude').count().where('count >1').count()
    print("Duplicate records count {}".format(duplicate_record_count))
    Coordinates_processed_df=Coordinates_processed_df.distinct()
    distinct_record_count=Coordinates_processed_df.count()
    print("Distinct records count {}".format(distinct_record_count))
    print("Duplicate records removed successfully")
except Exception as ex:
    print("Unable to remove duplicates")

### Raw data cleansing/ Source data validation ends here.


####CDC Logic implementation, this step helps to find out the distinct combination of longitude and latitude present in the raw data set of the current date, and then compares the active combination of longitude and latitude present in the curated data set

Coordinates_processed_df.createOrReplaceTempView("coordinates_raw")
#curated_df=spark.read.parquet(f'{mount_point}/{curated_folder_path}/{Module}'/coordinates.parquet')
#curated_df.createOrReplaceTempView("coordinates_curated")
#delta_records = spark.sql("select distinct longitude,latitude from coordinates_raw minus select distinct longitude,latitude from coordinates_curated union all select distinct longitude,latitude from coordinates_curated minus select distinct longitude,latitude from coordinates_raw ")
#Once the delta_records_df is prepared apply only those combination of longitude and latitude for next transformations

###Calling the common functions####
udf1=F.udf(lambda x,y: geohash.encode(x,y))
udf2=F.udf(lambda x,y,z:unique_prefix(x,y,z))

####Data processing/transformation starts here####
try:

    Coordinates_processed_df= Coordinates_processed_df.withColumn("Latitude",Coordinates_processed_df.Latitude.cast(DoubleType())).withColumn("Longitude",Coordinates_processed_df.Longitude.cast(DoubleType()))
    Coordinates_processed_df=Coordinates_processed_df.withColumn("geohash",udf1('Latitude','Longitude'))
    Coordinates_processed_df=Coordinates_processed_df.orderBy("geohash")
    Coordinates_processed_df=Coordinates_processed_df.withColumn("next_geocode", lead("geohash",1).over(Window.orderBy("geohash"))).withColumn("previous_geocode", lag("geohash",1).over(Window.orderBy("geohash")))
    Coordinates_processed_df=Coordinates_processed_df.withColumn("previous_geocode",coalesce('previous_geocode', lit('AAAAAAAAAAAA'))).withColumn("next_geocode",coalesce('next_geocode', lit('AAAAAAAAAAAA')))
    print("Data processing successfully completed")
except exception as ex:
    print("One or more Data processing steps failed")

### Calling the unique_prefix function####
try:
    Coordinates_processed_df=Coordinates_processed_df.withColumn("unique_prefix",udf2('geohash','previous_geocode','next_geocode'))
    print('Unique prefix Function completed successfully')
except Exception as ex:
    print("Unique prefix Function failed")


##Drop unwanted/extra fields from Dataframe####
try:
    Coordinates_processed_df=Coordinates_processed_df.drop("next_geocode","previous_geocode")
    print("Unwanted fields dropped from Dataframe")
except Exception as ex:
    print("Failed to drop unwanted fields")


###Data validation check: if there are any duplicates in unique prefix
Duplicate_record_count=Coordinates_processed_df.groupby('unique_prefix').count().where('count >1').sort('count', ascending=False).count()

if Duplicate_record_count==0:
    print('Process runs successfully and no duplicate entries found in unique_prefix')
else:
    print('Process runs successfully but duplicate records found in unique_prefix')

###Create the file in parquet format in the processed folder###

try:
    Coordinates_processed_df.write.mode('overwrite').parquet(f'{mount_point}/{processed_folder_path}/{Module}')
    print("Processed file successfully created")
except Exception as ex:
    print("Failed to create processed file in DBFS")



###Script ends here



