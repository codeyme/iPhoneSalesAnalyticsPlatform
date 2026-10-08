from pyspark.sql import SparkSession
from extract_bronze import *
from transform_silver import *
from load_gold import *

path = 'file:///home/takeo/startApps/localFiles/'
# files = ['customers.csv','products.csv','stores.csv','sales.csv']
# bronze_tables = ['bronze_customers','bronze_products','bronze_stores','bronze_sales']
entity = ['customers','products','stores','sales']

if __name__ == '__main__':
    spark:SparkSession = SparkSession.builder.master("local[1]").appName('iphoneSales').config('spark.sql.warehouse.dir','hdfs:///warehouse/tablespace/managed/hive').enableHiveSupport().getOrCreate()
    spark.sql("CREATE DATABASE IF NOT EXISTS iphone_analytics")
    spark.sql("USE iphone_analytics")

    # for name in entity:
    #     bronze_ingestion(spark,path,name)
    # bronze_ingestion(spark, path, 'customers')

    # silver_customers_transform(spark)
    # silver_products_transform(spark)
    # silver_stores_transform(spark)
    # silver_sales_transform(spark)
    # create_dim(spark)
    # create_fact(spark)
    load_fact(spark)
    load_dimensions(spark)