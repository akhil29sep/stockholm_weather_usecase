import json
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import *
from awsglue.dynamicframe import DynamicFrame


################## temperature file schemas ######################################

temp_obs_1756_1858 =StructType([ \
    StructField("year",IntegerType(),True), \
    StructField("month",IntegerType(),True), \
    StructField("day",IntegerType(),True), \
    StructField("morning_reading", StringType(), True), \
    StructField("noon_reading", StringType(), True), \
    StructField("evening_reading", StringType(), True) \
    ])


temp_obs_1859_1960 =StructType([ \
    StructField("year",IntegerType(),True), \
    StructField("month",IntegerType(),True), \
    StructField("day",IntegerType(),True), \
    StructField("morning_reading", StringType(), True), \
    StructField("noon_reading", StringType(), True), \
    StructField("evening_reading", StringType(), True), \
    StructField("tmin", StringType(), True), \
    StructField("tmax", StringType(), True) \
    ])

temp_obs_1961_2012 =StructType([ \
    StructField("year",IntegerType(),True), \
    StructField("month",IntegerType(),True), \
    StructField("day",IntegerType(),True), \
    StructField("morning_reading", StringType(), True), \
    StructField("noon_reading", StringType(), True), \
    StructField("evening_reading", StringType(), True), \
    StructField("tmin", StringType(), True), \
    StructField("tmax", StringType(), True), \
    StructField("mean", StringType(), True) \
    ])


temp_obs_2013_2017 = temp_obs_1961_2012




###########################################################################################################################

######################barometer file schema ######################################################

barometer_1756_1858 =StructType([ \
    StructField("year",IntegerType(),True), \
    StructField("month",IntegerType(),True), \
    StructField("day",IntegerType(),True), \
    StructField("pressure_morning_reading", StringType(), True), \
    StructField("temp_morning_reading", StringType(), True), \
    StructField("pressure_noon_reading", StringType(), True), \
    StructField("temp_noon_reading", StringType(), True), \
    StructField("pressure_evening_reading", StringType(), True) , \
    StructField("temp_evening_reading", StringType(), True) \
    ])

barometer_1859_1861 =StructType([ \
    StructField("year",IntegerType(),True), \
    StructField("month",IntegerType(),True), \
    StructField("day",IntegerType(),True), \
    StructField("pressure_morning_reading", StringType(), True), \
    StructField("temp_morning_reading", StringType(), True), \
    StructField("p_reduced_morning_reading", StringType(), True), \
    StructField("pressure_noon_reading", StringType(), True), \
    StructField("temp_noon_reading", StringType(), True), \
    StructField("p_reduced_noon_reading", StringType(), True), \
    StructField("pressure_evening_reading", StringType(), True), \
    StructField("temp_evening_reading", StringType(), True) , \
    StructField("p_reduced_evening_reading", StringType(), True) \
    ])

barometer_1862_1937 =StructType([ \
    StructField("year",IntegerType(),True), \
    StructField("month",IntegerType(),True), \
    StructField("day",IntegerType(),True), \
    StructField("pressure_morning_reading", StringType(), True), \
    StructField("pressure_noon_reading", StringType(), True), \
    StructField("pressure_evening_reading", StringType(), True) \
    ])

barometer_1938_1960 = barometer_1862_1937
barometer_1961_2012 = barometer_1862_1937
barometer_2013_2017 = barometer_1862_1937




unit_config_Temprature ={temp_obs_1756_1858 :"degreeC",temp_obs_1859_1960:"degreeC",temp_obs_1961_2012 :"degreeC",temp_obs_2013_2017:"degreeC" }
unit_config_Air_pressure ={barometer_1756_1858 :"SwedishInches",barometer_1859_1861:"SwedishInches*.1",barometer_1862_1937:"mmhg",barometer_1938_1960:"hpa" ,barometer_1961_2012:"hpa",barometer_1961_2012:"hpa"}

def extract_file_names(bucket,dir):
    s3 = boto3.resource("s3")
    s3_bucket = s3.Bucket(bucket)
    files_in_s3 = [f.key.rsplit('/',1)[1].replace('.txt','') for f in
                   s3_bucket.objects.filter(Prefix=dir).all()]
    return files_in_s3


def create_table_in_catlog(glue_context,final_df,source,publish_table_path):
    sink = glue_context.getSink(connection_type="s3", path =publish_table_path,
                               enableUpdateCatalog=True, updateBehavior="UPDATE_IN_DATABASE",
                               partitionKeys=["year"])
    sink.setFormat("glueparquet")
    sink.setCatalogInfo(catalogDatabase='akhil_test_db', catalogTableName=source)
    sink.writeFrame(final_df)


def load_raw_data_to_parquet(source):
    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)
    spark = glue_context.spark_session

    config =globals()["unit_config_"+source]
    bucket = 'eu-dev-akhil-data'
    base_path = "raw_data_csv/"
    s3_base_location ="s3://"+bucket+'/'+base_path + source
    output_path = "s3://"+bucket+'/'+'transformed/'+source+'/'
    temp_file = extract_file_names(bucket ,base_path+source+"/")
    # write data in transformed bucket
    for file in temp_file:
        file_schema =file.split('.',1)[0]
        df_with_schema = spark.read.format("csv") \
            .option("header", False) \
            .option("delimiter",",") \
            .schema(globals()[file_schema]) \
            .load(s3_base_location+'/'+file)
        df_new =df_with_schema.withColumn('unit' , lit(config[globals()[file_schema]])) \
            .replace('NaN',None)
        #df_new.write.format("parquet").mode("overwrite").partitionBy('year').save(output_path)
        df_new.write.format("parquet").mode('append').save(output_path)

    #publish data to catlog and atehna
    publish_table_path ="s3://"+bucket+'/'+'publish/'+source+'/'
    mergedDF = spark.read.option("mergeSchema", "true").parquet(output_path)
    final_df =DynamicFrame.fromDF(mergedDF,glue_context,'final_df')
    create_table_in_catlog(glue_context,final_df,source,publish_table_path)


def main():
    load_raw_data_to_parquet('Temprature')
    load_raw_data_to_parquet('Air_pressure')


if __name__ == "__main__":
    main()
