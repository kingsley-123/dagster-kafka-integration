import json
import time
from typing import List, Dict, Any
from confluent_kafka import KafkaError
from dagster import ConfigurableIOManager, InputContext, OutputContext, ResourceDependency
from pydantic import Field
from dagster_kafka.resources import KafkaResource

class KafkaIOManager(ConfigurableIOManager):
    """IO Manager for reading JSON data from Kafka topics."""
    
    kafka_resource: ResourceDependency[KafkaResource]
    consumer_group_id: str = Field(default="dagster-consumer")
    max_messages: int = Field(default=100)
    
    def load_input(self, context: InputContext) -> List[Dict[str, Any]]:
        """Load JSON data from Kafka topic."""
        topic = context.asset_key.path[-1] if context.asset_key else "default"
        context.log.info(f"Starting Kafka consumption from topic: {topic}")
        
        consumer = self.kafka_resource.get_consumer(self.consumer_group_id)
        consumer.subscribe([topic])
        context.log.info("Subscribed to topic, waiting for assignment...")
        
        # Give consumer time to connect and get assignment
        time.sleep(2)
        
        messages = []
        attempts = 0
        
        while len(messages) < self.max_messages and attempts < 20:
            msg = consumer.poll(timeout=2.0)  # Longer timeout
            attempts += 1
            
            if msg is None:
                context.log.debug(f"No message on attempt {attempts}")
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    context.log.debug("Reached end of partition")
                    break
                else:
                    raise Exception(f"Kafka error: {msg.error()}")
            
            try:
                data = json.loads(msg.value().decode("utf-8"))
                messages.append(data)
                context.log.info(f"Successfully parsed message {len(messages)}: {data}")
            except json.JSONDecodeError as e:
                context.log.warning(f"Failed to parse JSON: {e}")
                continue
        
        consumer.close()
        context.log.info(f"Finished: loaded {len(messages)} messages from topic {topic}")
        return messages

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        """Kafka IO manager is read-only for now."""
        pass
