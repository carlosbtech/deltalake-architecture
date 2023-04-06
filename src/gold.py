from aux import aux
from aux.query import GOLD_QUERY

def data_process_silver_to_gold():
    spark = aux.create_spark_session('gold')

    data_path = 'datalake/silver/dataset_user_payments_silver'
    df_user_payment = aux.read_data(spark, data_path, 'delta')
    df_user_payment.createOrReplaceTempView('vw_user_payment')

    dataset = spark.sql(GOLD_QUERY)
    delta_gold_store_zone = 'datalake/gold/dataset_user_payments_gold/'
    aux.write_data(dataset, delta_gold_store_zone, 'overwrite')

    spark.stop()