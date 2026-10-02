from pyspark.sql import SparkSession
from Extract_bronze import *


path = 'files:///home/takeo/startApps/localFiles/'
# files = ['customers.csv','products.csv','stores.csv','sales.csv']
# bronze_tables = ['bronze_customers','bronze_products','bronze_stores','bronze_sales']
entity = ['customers','products','stores','sales']
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    spark:SparkSession = SparkSession.Builder.master('local[1]').appName('iphoneSales').config('spark.sql.warehouse.dir','hdfs:///warehouse/tablespace/managed/hive').enableHiveSupport().getOrCreate()
    for name in entity:
        bronze_ingestion(spark,path,name)
