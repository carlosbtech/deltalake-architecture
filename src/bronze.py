from aux import aux

def data_transfer_landing_to_bronze():
    spark = aux.create_spark_session('bronze')
    
    tables = ['vehicle', 'subscription', 'movies' , 'user']
    
    for table in tables:
        data = f'datalake/landing/{table}/*.json'
        
        df = aux.read_data(spark, data, 'json')

        delta_processing_store_zone = f'datalake/bronze/{table}_bronze'
        aux.write_data(df, delta_processing_store_zone, 'append')

    spark.stop()