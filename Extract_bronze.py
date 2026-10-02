def bronze_ingestion(spark,csv_path,table_name):
    df = (spark.read.option('header','true').csv(csv_path))
    df.write.mode('overwrite').format('parquet').saveAsTable(f'bronze_{table_name}')
    return f'bronze_{table_name}'