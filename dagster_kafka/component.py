from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import dagster as dg
from dagster import AssetSpec, asset, AssetExecutionContext

from .resources import KafkaResource, SecurityProtocol, SaslMechanism
from .io_manager import KafkaIOManager
from .avro_io_manager import AvroKafkaIOManager
from .protobuf_io_manager import create_protobuf_kafka_io_manager
from .dlq import DLQStrategy


@dataclass
class KafkaConfig:
    """Configuration for Kafka connection."""
    bootstrap_servers: str
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None
    ssl_ca_location: Optional[str] = None
    ssl_check_hostname: bool = True


@dataclass
class ConsumerConfig:
    """Configuration for Kafka consumer."""
    consumer_group_id: str = "dagster-consumer"
    max_messages: int = 100
    enable_dlq: bool = True
    dlq_strategy: str = "RETRY_THEN_DLQ"
    dlq_max_retries: int = 3


@dataclass 
class TopicConfig:
    """Configuration for a Kafka topic."""
    name: str
    format: str  # "json", "avro", or "protobuf"
    schema_registry_url: Optional[str] = None
    asset_key: Optional[str] = None


@dataclass
class KafkaComponent(dg.Component):
    """
    A Dagster Component for Kafka integration.
    
    Allows teams to configure Kafka assets via YAML instead of Python code.
    """
    
    kafka_config: KafkaConfig
    consumer_config: ConsumerConfig
    topics: List[TopicConfig]
    
    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """Build Dagster definitions from the component configuration."""
        
        # Create KafkaResource from config
        kafka_resource = KafkaResource(
            bootstrap_servers=self.kafka_config.bootstrap_servers,
            security_protocol=SecurityProtocol(self.kafka_config.security_protocol),
            sasl_mechanism=SaslMechanism(self.kafka_config.sasl_mechanism) if self.kafka_config.sasl_mechanism else None,
            sasl_username=self.kafka_config.sasl_username,
            sasl_password=self.kafka_config.sasl_password,
            ssl_ca_location=self.kafka_config.ssl_ca_location,
            ssl_check_hostname=self.kafka_config.ssl_check_hostname,
        )
        
        assets = []
        resources = {"kafka": kafka_resource}
        
        # Create assets for each topic
        for topic in self.topics:
            asset_key = topic.asset_key or topic.name.replace("-", "_")
            
            if topic.format.lower() == "json":
                # Create JSON IO Manager
                io_manager = KafkaIOManager(
                    kafka_resource=kafka_resource,
                    consumer_group_id=self.consumer_config.consumer_group_id,
                    max_messages=self.consumer_config.max_messages,
                    enable_dlq=self.consumer_config.enable_dlq,
                    dlq_strategy=DLQStrategy(self.consumer_config.dlq_strategy),
                    dlq_max_retries=self.consumer_config.dlq_max_retries,
                )
                
                resources[f"{asset_key}_io_manager"] = io_manager
                
                # Create asset function
                def create_json_asset(topic_name=topic.name, key=asset_key):
                    @asset(
                        key=key,
                        description=f"Kafka JSON topic: {topic_name}",
                        io_manager_key=f"{key}_io_manager"
                    )
                    def kafka_json_asset(context: AssetExecutionContext):
                        """Load JSON data from Kafka topic."""
                        return {"topic": topic_name, "format": "json"}
                    
                    return kafka_json_asset
                
                assets.append(create_json_asset())
                
            elif topic.format.lower() == "avro":
                if not topic.schema_registry_url:
                    raise ValueError(f"schema_registry_url required for Avro topic: {topic.name}")
                
                # Create Avro IO Manager  
                io_manager = AvroKafkaIOManager(
                    kafka_resource=kafka_resource,
                    schema_registry_url=topic.schema_registry_url,
                    consumer_group_id=self.consumer_config.consumer_group_id,
                    max_messages=self.consumer_config.max_messages,
                    enable_dlq=self.consumer_config.enable_dlq,
                    dlq_strategy=DLQStrategy(self.consumer_config.dlq_strategy),
                    dlq_max_retries=self.consumer_config.dlq_max_retries,
                )
                
                resources[f"{asset_key}_io_manager"] = io_manager
                
                def create_avro_asset(topic_name=topic.name, key=asset_key):
                    @asset(
                        key=key,
                        description=f"Kafka Avro topic: {topic_name}",
                        io_manager_key=f"{key}_io_manager"
                    )
                    def kafka_avro_asset(context: AssetExecutionContext):
                        """Load Avro data from Kafka topic."""
                        return {"topic": topic_name, "format": "avro"}
                    
                    return kafka_avro_asset
                
                assets.append(create_avro_asset())
                
            elif topic.format.lower() == "protobuf":
                if not topic.schema_registry_url:
                    raise ValueError(f"schema_registry_url required for Protobuf topic: {topic.name}")
                
                # Create Protobuf IO Manager
                io_manager = create_protobuf_kafka_io_manager(
                    kafka_resource=kafka_resource,
                    schema_registry_url=topic.schema_registry_url,
                    consumer_group_id=self.consumer_config.consumer_group_id,
                    max_messages=self.consumer_config.max_messages,
                    enable_dlq=self.consumer_config.enable_dlq,
                    dlq_strategy=DLQStrategy(self.consumer_config.dlq_strategy),
                    dlq_max_retries=self.consumer_config.dlq_max_retries,
                )
                
                resources[f"{asset_key}_io_manager"] = io_manager
                
                def create_protobuf_asset(topic_name=topic.name, key=asset_key):
                    @asset(
                        key=key,
                        description=f"Kafka Protobuf topic: {topic_name}",
                        io_manager_key=f"{key}_io_manager"
                    )
                    def kafka_protobuf_asset(context: AssetExecutionContext):
                        """Load Protobuf data from Kafka topic."""
                        return {"topic": topic_name, "format": "protobuf"}
                    
                    return kafka_protobuf_asset
                
                assets.append(create_protobuf_asset())
        
        return dg.Definitions(
            assets=assets,
            resources=resources,
        )