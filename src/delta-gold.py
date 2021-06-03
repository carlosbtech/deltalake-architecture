import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import DataFrame

if __name__ == '__main__':

    spark = pyspark.sql.SparkSession.builder.appName("Pyspark - Deltalake")\
    .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .getOrCreate()

    #spark.sparkContext.setLogLevel("INFO")
    
    get_dataset = "/Users/carlosbarbosa/Desktop/work/challenge-ows/delta/silver/dataset_user_payments"

    def read_data(path: str, format: str) -> DataFrame:
        """[Read data on local path]

        Args:
            path (str): [data location path]

        Returns:
            DataFrame: [Dataframe with data]
        """        
        df = spark.read.format(format)\
            .option("inferSchema", "true")\
            .option("header", "true")\
            .option("versionAsOf", "0")\
            .load(path)

        return df

    def write_data(df: DataFrame, path: str, write_delta_mode: str) -> str:
        """[Write data on format delta]

        Args:
            df (DataFrame): [Dataframe with data]
            path (str): [data location path]
            write_delta_mode (str): [append, overwrite]

        Returns:
            str: [Message after write]
        """        
        df.write.mode(write_delta_mode)\
        .format("delta")\
        .save(path)

        return "Data saved successfully"

    # Read data of vehicle join with subscription
    df_user_payment = read_data(get_dataset, "delta")
    df_user_payment.printSchema()
    df_user_payment.createOrReplaceTempView("vw_user_payment")

    dataset = spark.sql("""
            SELECT
            full_name,
            email_user,
            floor(datediff(now(),date_of_birth)/365.25) as age,
            CASE 
                WHEN floor(datediff(now(),date_of_birth)/365.25) <= 21 THEN 'young' 
                WHEN floor(datediff(now(),date_of_birth)/365.25) <= 51 THEN 'adult'
                ELSE 'elder' end as age_category,
            CASE 
                WHEN payment_method is null then 'Payment not made'
                ELSE payment_method
            END as payment_method,
            CASE 
                WHEN status is null then 'N/D'
                ELSE status
            END as status
            FROM
                vw_user_payment
            WHERE status IN ("Blocked", "Pending")
            """)
    delta_gold_store_zone = f"/Users/carlosbarbosa/Desktop/work/challenge-ows/delta/gold/dataset_user_payments_gold/"
    write_data(dataset, delta_gold_store_zone, "overwrite")
    dataset.show()

    spark.stop()