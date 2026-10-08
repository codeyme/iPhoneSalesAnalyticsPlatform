def bronze_ingestion(spark,csv_path,table_name):
    df = spark.read.option('header','true').csv(f'{csv_path}{table_name}.csv') #hdfs read
    df.write.mode('overwrite').format('parquet').saveAsTable(f'bronze_{table_name}') # hive dump using saveAsTable()
    print(f'bronze_{table_name}')
    # return f'bronze_{table_name}'