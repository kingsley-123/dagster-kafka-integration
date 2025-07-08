from typing import Dict, Any
from confluent_kafka import Consumer
from dagster import ConfigurableResource
from pydantic import Field

class KafkaResource(ConfigurableResource):
    """Resource for connecting to Kafka cluster."""
    
    bootstrap_servers: str = Field(
        description="Kafka bootstrap servers (e.g., 'localhost:9092')"
    )
    
    def get_consumer(self, group_id: str) -> Consumer:
        """Create a Kafka consumer."""
        config = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
        }
        return Consumer(config)