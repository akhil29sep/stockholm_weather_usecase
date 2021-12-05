# # stockholm_weather_usecase

# Tools used
1. Terraform
2. AWS Glue -Processing
3. AWS s3 - storage
4. AWS Athena - Analysis/publish

# Script : terraform/main.tf
1. Created s3 bucket and blocked access
2. Sync local code and data folder with s3 location
3. Created glue service role and bucket polciy around it
4. created Glue job
5. created Database in catalog

# Script : utilties/raw_to_csv.py
Cleaned raw file to csv format (Need to do this as txt files were not delimited)
script read from raw_data folder and create csv in raw_data_csv. Aws sync command in terraform read from this location and upload data in s3.

# pyspark_Code/pyscript.py
 glue spark script :
1. Read from raw data and apply schema to each file and save it as parquet
2. Merge and read data as 1 dataset and create table in Catlog to be queried by athena.


