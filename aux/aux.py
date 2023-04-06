from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

def create_spark_session(app_name: str) -> SparkSession:
    '''[Create or get a SparkSession]

    Args:
        app_name (str): [Name of the Spark application]

    Returns:
        SparkSession: [A SparkSession object]
    '''
    spark = (
        SparkSession.builder
            .appName(app_name)
            .config('spark.jars.packages', 'io.delta:delta-core_2.12:2.3.0')
            .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
            .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
            .getOrCreate()
    )

    spark.sparkContext.setLogLevel('ERROR')
    
    return spark


def read_data(spark: SparkSession, path: str, format: str) -> DataFrame:
    '''[Read data from the given path]

    Args:
        spark (SparkSession): [SparkSession object]
        path (str): [data location path]
        format (str): [file format]

    Returns:
        DataFrame: [Dataframe with data]
    '''        
    df = (
        spark.read
            .format(format)
            .option('inferSchema', 'true')
            .option('header', 'true')
            .load(path)
    )

    return df


def write_data(df: DataFrame, path: str, write_mode: str) -> None:
    '''[Write data to the given path]

    Args:
        df (DataFrame): [Dataframe with data]
        path (str): [data location path]
        write_mode (str): [append, overwrite]
    '''        
    (
        df.write
            .mode(write_mode)
            .format('delta')
            .save(path)
    )