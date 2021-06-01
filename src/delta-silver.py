import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import DataFrame

if __name__ == '__main__':

    spark = pyspark.sql.SparkSession.builder.appName("Pyspark - Deltalake")\
    .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")
   
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

    # Array of tables
    table_names = ["vehicle", "subscription", "movies" , "user"]

    for table_name in table_names:
        data = f"/Users/carlosbarbosa/Desktop/work/challenge-ows/delta/bronze/{table_name}"
        df = read_data(data, "delta")
        df.createOrReplaceTempView(table_name)

    df = spark.sql("""
            SELECT
                CONCAT(vu.first_name, ' ' ,vu.last_name)  as full_name,
                vu.email                                  as email_user,
                CASE 
                    WHEN vu.gender is null THEN 'N/D' 
                    ELSE vu.gender
                END                                       as gender_user,
                vu.username,
                vu.date_of_birth,
                vv.car_type,
                vv.color,
                vs.payment_method,
                vs.status,
                vv.dt_current_timestamp
            FROM
                user                as vu
            INNER JOIN
                vehicle             as vv
            ON  vu.user_id = vv.user_id
            LEFT JOIN 
                subscription as vs
            ON  vu.user_id = vs.user_id
            """)
    delta_processing_store_zone = f"/Users/carlosbarbosa/Desktop/work/challenge-ows/delta/silver/dataset_user_payments/"
    write_data(df, delta_processing_store_zone, "append")

    spark.stop()