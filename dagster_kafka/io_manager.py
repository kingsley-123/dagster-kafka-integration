import json
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
        # Use asset name as topic name
        topic = context.asset_key.path[-1] if context.asset_key else "default"
        
        consumer = self.kafka_resource.get_consumer(self.consumer_group_id)
        consumer.subscribe([topic])
        
        messages = []
        for _ in range(self.max_messages):
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    break
                else:
                    raise Exception(f"Kafka error: {msg.error()}")
            
            # Parse JSON message
            try:
                data = json.loads(msg.value().decode('utf-8'))
                messages.append(data)
            except json.JSONDecodeError:
                continue
                
        consumer.close()
        context.log.info(f"Loaded {len(messages)} messages from topic {topic}")
        return messages
    
    def handle_output(self, context: OutputContext, obj: Any) -> None:
        """Kafka IO manager is read-only for now."""
        pass