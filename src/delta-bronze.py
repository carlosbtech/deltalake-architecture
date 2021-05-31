import os
import pyspark
from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = pyspark.sql.SparkSession.builder.appName("Pyspark - Deltalake") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")
   
    get_credit_card_data = "/Users/carlosbarbosa/Desktop/work/de-apache-spark/files/landing-zone/credit_card/*.json"

    df = (
    spark.read\
    .format("json") \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .json(get_credit_card_data)
    )

    df.createOrReplaceTempView("vw_credit_card")

    df = spark.sql("select * from vw_credit_card where credit_card_type = 'mastercard'")
    
    df.printSchema()
    df.show()

    write_delta_mode = "append"
    delta_processing_store_zone = "/Users/carlosbarbosa/Desktop/work/deltalake-poc/delta/bronze"

    df.write \
        .mode(write_delta_mode) \
        .format("delta") \
        .save(delta_processing_store_zone + "/credit_card/")