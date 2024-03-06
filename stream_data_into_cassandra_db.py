import logging, sys

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, FloatType


# Create a schema for the cassandra database
def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS air_gradient
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print('Keyspace created successfully!')

# Create a table for the session
def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS air_gradient.metrics_api (
        id UUID PRIMARY KEY,
        timestamp TIMESTAMP,
        wifi FLOAT,
        rco2 INT,
        pm02 INT,
        tvoc INT,
        temp_celsius FLOAT,
        temp_fahrenheit FLOAT,
        rhum INT);
    """)

    print("Table created successfully!")

# This function will insert the data into the Cassandra db
def insert_data(session, **kwargs):
    print("Inserting data...")

    # Map the values from Kafka to the columns created in Cassandra
    id = kwargs.get('id')
    timestamp = kwargs.get('timestamp')
    wifi = kwargs.get('wifi')
    rco2 = kwargs.get('rco2')
    pm02 = kwargs.get('pm02')
    tvoc = kwargs.get('tvoc')
    temp_celsius = kwargs.get('atmp')
    temp_fahrenheit = (kwargs.get('atmp') * 1.8) + 32
    rhum = kwargs.get('rhum')

    try:
        session.execute("""
            INSERT INTO air_gradient.metrics_api(id, timestamp, wifi, rco2, pm02, 
                tvoc, temp_celsius, temp_fahrenheit, rhum)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (id, timestamp, wifi, rco2, pm02, 
                tvoc, temp_celsius, temp_fahrenheit, rhum))
        logging.info(f"Data inserted for {id} at {timestamp}")

    except Exception as e:
        logging.error(f'Unable to insert data due to {e}')

# This will generate the spark connection
def create_spark_connection():
    # Define the variable for the spark connector
    s_conn = None
    try:
        # Build the spark connection to cassandra db using spark cassandra connector from maven repo
        # TODO: Check if the config needs to be modified over to Kafka instead of using localhost
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,"
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()
        # Throw an error if there is an issue with the connection.
        s_conn.sparkContext.setLogLevel("ERROR")
        print('created spark connection')
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")
        print('spark connection failed')
    
    # Check if there is a connection that was built, otherwise return None
    return s_conn

def connect_to_kafka(spark_conn):
    spark_df = None
    # Try to create the connection to Kafka via the spark connector
    try:
        # Set the bootstrap server equal to value used for internal container routing
        spark_df = spark_conn.readStream.format('kafka').option('kafka.bootstrap.servers', 'localhost:9094').option('subscribe', 'air_gradient_metrics').option('startingOffsets', 'earliest').load()
        logging.info("The Kafka dataframe has been successfully created!")
    # Log the exception if one occurs
    except Exception as e:
        logging.warning(f"Unable to create Kafka dataframe due to the following issue: {e}")
        sys.exit()
    return spark_df

# This will generate the cassandra db connection.
def create_cassandra_connection():
    try:
        # Connect to the cassandra cluster
        cluster = Cluster(['localhost'])
        # Attempt to create a session
        cas_session = cluster.connect()
        # Return the session
        return cas_session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to the following: {e}")
        return None

def create_selection_df_from_kafka(spark_df):
    # Set the schema of the JSON output from Kafka
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("wifi", FloatType(), False),
        StructField("rco2", IntegerType(), False),
        StructField("pm02", IntegerType(), False),
        StructField("tvoc", IntegerType(), False),
        StructField("atmp", FloatType(), False),
        StructField("rhum", IntegerType(), False)
    ])
    
    select_statement = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(select_statement)

    return select_statement

if __name__ == "__main__":
    spark_conn = create_spark_connection()

    if spark_conn:
        # Get the data frame via connection to Kafka 
        spark_df = connect_to_kafka(spark_conn)
        # Select the pertinent data from the dataframe
        selection_df = create_selection_df_from_kafka(spark_df)
        # Create a connection to the cassandra db instance and store the session
        session = create_cassandra_connection()

        if session:
            create_keyspace(session)
            create_table(session)
            
            logging.info("Streaming is being started...")
            
            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'air_gradient')
                               .option('table', 'metrics_api')
                               .start())
            
            streaming_query.awaitTermination()