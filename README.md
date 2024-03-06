<h1>Air Gradient Metrics</h1>

<h2>Description - WIP</h2>
End to end implementation to pull real time data from the AirGradient sensor, including values such as PM2.5, CO2, Temperature, and Humidity. Data is pulled using Airflow into Kafka and will be ingested via Spark and stored in CassandraDB. Real-time reporting will be generated via Grafana. 

<h2>Installation Stpes</h2>
<ol>1. Install Docker on your system.</ol>
<ol>2. Clone the repo to your host.</ol>
<ol>3. Ensure proper airflow permissions have been granted by running <code>echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env</code> and granting elevated permissions to script/entrypoint.sh <code>chmod +x entrypoint.sh </code>. Grant write permissions for airflow to logs, dags, and plugins (e.g. <code>chmod -R 777 dags/</code>).</ol>
<ol>4. Execute the <code>sudo docker compose up -d</code> to instantiate all of the services.</ol> 
<ol>5. Ensure that all services are successfully up and running.</ol> 
<ol>6. Access Airflow at host instance ip address at <code>port 8080</code>, enable the DAG, and then trigger it.</ol> 
<ol>7. Verify that messages are being appended to topic <code>air_gradient_metrics</code>.</ol> 
<ol>8. Install spark cassandra & spark sql kafka drivers <code>air_gradient_metrics</code>.</ol> 


<h2>Useful Terminal Commands</h2>
<h3>Kafka Container</h3>
Enter the container in interactive mode: <code>"sudo docker exec -it kafka_broker /bin/sh"</code></br></br>
Change directories to binaries folder: <code>"cd opt/bitnami/kafka/bin"</code></br></br>
Create Kafka Topic (specific to repo): <code>"./kafka-topics.sh --create --topic air_gradient_metrics --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1"</code></br></br>
List of all Topics within the container: <code>"kafka-topics.sh --bootstrap-server=localhost:9092 --list"</code></br></br>
Grab all the messages pushed to a topic from the beginning: <code>"kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic air_gradient_metrics --from-beginning"</code>

<h3>Docker</h3>
Build a docker image from a Dockerfile (don't forget the "."): <code>"docker build -t [user inputted topic name] ."</code><br/><br/>
Run the docker image within a pre-existing network: <code>"docker run --network=[name of network] [name of image]"</code>

