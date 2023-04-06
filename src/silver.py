from aux import aux
from aux.query import SILVER_QUERY

def data_process_bronze_to_silver():
    spark = aux.create_spark_session('silver')

    tables = ['vehicle', 'subscription', 'movies' , 'user']

    for table in tables:
        data_path = f'datalake/bronze/{table}_bronze'
        df = aux.read_data(spark, data_path, 'delta')
        df.createOrReplaceTempView(table)

    df = spark.sql(SILVER_QUERY)
    delta_processing_store_zone = 'datalake/silver/dataset_user_payments_silver'
    aux.write_data(df, delta_processing_store_zone, 'append')

    spark.stop()