from dagster_kafka import KafkaResource, KafkaIOManager

kafka_resource = KafkaResource(bootstrap_servers="localhost:9092")
io_manager = KafkaIOManager(
    kafka_resource=kafka_resource,
    consumer_group_id="test"
)

print("Methods in KafkaIOManager:")
for method in dir(io_manager):
    if not method.startswith("__"):
        print(f"  - {method}")
