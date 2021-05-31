import os
import pyspark
from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = pyspark.sql.SparkSession.builder.appName("Pyspark - Deltalake") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

    #spark.sparkContext.setLogLevel("INFO")
   
    get_credit_card_data = "/Users/carlosbarbosa/Desktop/work/deltalake-poc/delta/bronze/credit_card/"

    olderDeltaTableDF = spark.read \
    .format("delta") \
    .option("versionAsOf", 0).load(get_credit_card_data)  #Time travel

    olderDeltaTableDF.createOrReplaceTempView("src_vw_credit_card")


    newDeltaTableDF = spark.read \
    .format("delta") \
    .option("versionAsOf", 1).load(get_credit_card_data)

    newDeltaTableDF.createOrReplaceTempView("tgt_vw_credit_card")

    newDeltaTableDF3 = spark.read \
    .format("delta") \
    .option("versionAsOf", 3).load(get_credit_card_data)

    newDeltaTableDF3.createOrReplaceTempView("df3")

    spark.sql("SELECT count(*) as qtd_rows_frame_version_0 from df3").show()

    spark.sql(
        """
        SELECT user_id, id, count(*) as qtd_rows
        FROM df3
        GROUP BY user_id, id
        HAVING COUNT(*) > 1
        """
    ).show()

    newDeltaTableDF.show()

    # Show duplicate rows
    spark.sql(
        """
        SELECT user_id, id, count(*) as qtd_rows
        FROM src_vw_credit_card
        GROUP BY user_id, id
        HAVING COUNT(*) > 1
        """
    ).show()

    spark.sql("SELECT count(*) as qtd_rows_frame_version_0 from src_vw_credit_card").show()

    olderDeltaTableDF.show()
    
    spark.sql(
        """
        SELECT user_id, id, count(*) as qtd_rows
        FROM tgt_vw_credit_card
        GROUP BY user_id, id
        HAVING COUNT(*) > 1
        """
    ).show()

    spark.sql("SELECT count(*) as qtd_rows_frame_version_1 from tgt_vw_credit_card").show()

    newDeltaTableDF.show()
    
    spark.stop()