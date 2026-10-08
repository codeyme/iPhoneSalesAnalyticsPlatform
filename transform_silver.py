from pyspark.sql.functions import col, to_date

def silver_customers_transform(spark):
    df = spark.table('bronze_customers')
    clean_df = (
        df.withColumn('customer_id', col('customer_id').cast('int'))
    )
    # spark.sql("USE iphone_analytics")
    (clean_df.write.mode('overwrite')
    .format('parquet').saveAsTable('silver_customers'))
    print('silver_customers')
    return 'silver_customers'

def silver_products_transform(spark):
    df = spark.table('bronze_products')
    clean_df = (
        df.withColumn('product_id', col('product_id').cast('int'))
        .withColumn('unit_price', col('unit_price').cast('int'))
                )
    (clean_df.write.mode('overwrite')
     .format('parquet').saveAsTable('silver_products'))
    print('silver_products')
    return 'silver_products'

def silver_stores_transform(spark):
    df = spark.table('bronze_stores')
    clean_df = (df.withColumn('store_id', col('store_id').cast('int')))
    (clean_df.write.mode('overwrite').format('parquet')
    .saveAsTable('silver_stores'))
    print('silver_stores')
    return 'silver_stores'

def silver_sales_transform(spark):
    df = spark.table('bronze_sales')
    clean_df = (
        df.withColumn('sale_id', col('sale_id').cast('int'))
        .withColumn('product_id', col('product_id').cast('int'))
        .withColumn('customer_id', col('customer_id').cast('int'))
        .withColumn('quantity', col('quantity').cast('int'))
        .withColumn('store_id', col('store_id').cast('int'))
        .withColumn('sale_date', to_date(col("sale_date")))
    )
    (clean_df.write.mode('overwrite').partitionBy('sale_date')
     .format('parquet').saveAsTable('silver_sales'))
    print('silver_sales')
    return 'silver_sales'
