from pyspark.sql.functions import col, year,month, dayofmonth

def create_dim(spark):
    spark.sql('''
    create table IF NOT EXISTS iphone_analytics.dim_customer(
        customer_id INT,
        customer_name STRING,
        city STRING,
        state STRING
    )
        stored as parquet;
    ''')
    spark.sql('''
    create table IF NOT EXISTS iphone_analytics.dim_product(
        product_id int,
        product_name string,
        category string,
        unit_price int
    )
        stored as parquet;
''')

    spark.sql('''
    create table IF NOT EXISTS iphone_analytics.dim_store(
        store_id int,
        store_name string,
        city string,
        state string
    )
        stored as parquet;
        ''')

    spark.sql('''
    create table IF NOT EXISTS iphone_analytics.dim_date(
        date_key date,
        year int,
        month int,
        day int
    )
        stored as parquet;
    ''')

def create_fact(spark):
    spark.sql('''
        create table IF NOT EXISTS iphone_analytics.fact_sales(
            sale_id int,
            customer_id int,
            product_id int,
            store_id int,
            quantity int,
            total_amount int
        )
        partitioned by (date_key date)
        stored as parquet;
    ''')
def load_dimensions(spark):
    print('starting')
    customers = (spark.table('silver_customers')
                 .select('*')
                 .dropDuplicates(['customer_id'])
                 )
    products = (spark.table('silver_products')
                 .select('*')
                 .dropDuplicates(['product_id'])
                 )
    stores = (spark.table('silver_stores')
                 .select('*')
                 .dropDuplicates(['store_id'])
                 )
    dates = (spark.table('silver_sales')
                 .select(col("sale_date").alias("date_key"))
                 .dropDuplicates(['date_key'])
                 .withColumn('year',year('date_key'))
                 .withColumn('month', month('date_key'))
                 .withColumn('day', dayofmonth('date_key'))
                 .select('date_key','year','month','day')
                 )
    print('done')
    customers.write.mode('overwrite').insertInto('dim_customer')
    products.write.mode('overwrite').insertInto('dim_product')
    stores.write.mode('overwrite').insertInto('dim_store')
    dates.write.mode('overwrite').insertInto('dim_date')

def load_fact(spark):
    sales = spark.table('silver_sales')
    products = spark.table('silver_products')

    fact_df = (
        sales.join(products, 'product_id')
        .withColumn('total_amount', col('quantity')*col('unit_price'))
        .select('sale_id','customer_id','product_id','store_id',
                    'quantity','total_amount',
                col('sale_date').alias('date-key'))
    )
    (fact_df.write.mode('overwrite').insertInto('fact_sales'))