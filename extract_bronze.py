def bronze_ingestion(spark,csv_path,table_name):
    df = spark.read.option('header','true').csv(f'{csv_path}{table_name}.csv')
    df.write.mode('overwrite').format('parquet').saveAsTable(f'bronze_{table_name}')
    print(f'bronze_{table_name}')
    return f'bronze_{table_name}'