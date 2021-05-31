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

    df = spark.read \
    .format("delta").load(get_credit_card_data)

    df.createOrReplaceTempView("vw_credit_card")

    olderDeltaTableDF = spark.read \
    .format("delta") \
    .option("versionAsOf", 0).load(get_credit_card_data)  #Time travel

    olderDeltaTableDF.createOrReplaceTempView("src_vw_credit_card")

    # Show duplicate rows
    spark.sql(
        """
        SELECT user_id, id, count(*)
        FROM vw_credit_card
        GROUP BY user_id, id
        HAVING COUNT(*) > 1
        """
    ).show()

    spark.sql(
        """
        SELECT user_id, id, count(*)
        FROM src_vw_credit_card
        GROUP BY user_id, id
        HAVING COUNT(*) > 1
        """
    ).show()

    # spark.sql("""
    #           MERGE INTO vw_credit_card vw_credit_card
    #           USING src_vw_credit_card
    #           ON src_vw_credit_card.user_id = vw_credit_card.user_id
    #           WHEN MATCHED AND vw_credit_card.row_flag = 'U'
    #           THEN
    #           UPDATE SET *
    #           WHEN MATCHED AND vw_credit_card.row_flag = 'D'
    #           THEN DELETE
    #           WHEN NOT MATCHED and vw_credit_card.row_flag = 'I'
    #           THEN INSERT *
    #           """).show()

    spark.stop()