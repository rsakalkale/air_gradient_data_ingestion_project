import json, logging, uuid, requests, pytz
from datetime import datetime
from airflow import DAG
from kafka import KafkaProducer
from time import sleep
from airflow.operators.python import PythonOperator

# Define the default args for the DAG
default_args = {
    'owner': 'rsakalkale',
    'start_date': datetime(2024, 3, 1, 12, 00)
}

# Initialize the serializer that will wrap data and encode it into utf-8
def serializer(message):
    return json.dumps(message, default=str).encode('utf-8')

def format_data(json_input):
    # Define an output var to enrich the data
    json_output = {}
    
    # Select the Los Angeles timezone
    pst_timezone = pytz.timezone('America/Los_Angeles') 
    
    # Add in a UUID and timestamp
    json_output['id'] = uuid.uuid4()
    json_output['timestamp'] = datetime.now(pst_timezone)

    # Grab all inputs and add them to enriched JSON output
    for k, v in json_input.items():
        json_output[k] = v

    return json_output

def get_data():
    # Request from the API of the AirGradient
    response = requests.get("http://192.168.0.103:9926/metrics")

    # Unpack JSON in to Python dictionary
    json_data = json.loads(response.content)
    return json_data

def stream_data():
    # Create the producer and add in the relevent keys
    producer = KafkaProducer(
        bootstrap_servers = ['kafka:9092'],
        value_serializer = serializer
    )
    
    # Infinite loop to continue grabbing the data
    while True:
        # Try querying the data from the API, formatting, and sending it
        try:
            # Grab the data and format it
            res = get_data()
            res = format_data(res)

            # Send the data to the topic
            producer.send('air_gradient_metrics', res)

            # Sleep for 5 seconds to not ovewhelm the system
            sleep(5)
            
        # Otherwise, end in an exception and log the error
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue

with DAG(
    dag_id = 'air_gradient_metrics',
    default_args = default_args,
    schedule_interval = '@daily',
    catchup = False
) as dag:

    streaming_task = PythonOperator(
        task_id = 'stream_data_from_api',
        python_callable = stream_data
    )            
    
