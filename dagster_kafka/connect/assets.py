from typing import Dict, Any, Optional, List
from dagster import asset, AssetExecutionContext, Config, ResourceParam

class ConnectorConfig(Config):
    """Configuration for a Kafka Connect connector asset."""
    
    name: str
    connector_class: str
    config: Dict[str, Any]
    
    def to_connect_config(self) -> Dict[str, Any]:
        """Convert to Kafka Connect API format."""
        connect_config = self.config.copy()
        connect_config["connector.class"] = self.connector_class
        connect_config["name"] = self.name
        return {
            "name": self.name,
            "config": connect_config
        }

def create_connector_asset(
    resource_key: str = "connect",
    group_name: str = "kafka_connect",
    key_prefix: Optional[List[str]] = None,
):
    """
    Factory function that creates a Dagster asset representing a Kafka Connect connector.
    
    Args:
        resource_key: The resource key for the ConfluentConnectResource
        group_name: The asset group name
        key_prefix: Optional prefix for the asset key
        
    Returns:
        A Dagster asset that manages a Kafka Connect connector
    """
    
    @asset(
        group_name=group_name,
        key_prefix=key_prefix,
        compute_kind="kafka_connect",
        required_resource_keys={resource_key},  # Add this to tell Dagster we need this resource
    )
    def connector_asset(
        context: AssetExecutionContext,
        config: ConnectorConfig,
    ) -> Dict[str, Any]:
        """
        A Dagster asset representing a Kafka Connect connector.
        
        This asset creates or updates a connector and returns its status.
        """
        # Get the connect resource from context
        connect = getattr(context.resources, resource_key)
        
        connector_name = config.name
        connect_config = config.to_connect_config()
        
        context.log.info(f"Managing connector: {connector_name}")
        
        # Check if connector exists
        connectors = connect.list_connectors()
        
        if connector_name in connectors:
            context.log.info(f"Updating existing connector: {connector_name}")
            current_config = connect.get_connector_config(connector_name)
            
            if current_config != connect_config["config"]:
                connect.update_connector_config(
                    connector_name, 
                    connect_config["config"]
                )
                context.log.info(f"Connector {connector_name} configuration updated")
            else:
                context.log.info(f"Connector {connector_name} configuration unchanged")
        else:
            context.log.info(f"Creating new connector: {connector_name}")
            connect.create_connector(connect_config)
            context.log.info(f"Connector {connector_name} created")
        
        # Get connector status
        try:
            status = connect.get_connector_status(connector_name)
            context.log.info(f"Connector {connector_name} status: {status['connector']['state']}")
            
            # Return connector info and status
            return {
                "name": connector_name,
                "state": status["connector"]["state"],
                "worker_id": status["connector"]["worker_id"],
                "type": status.get("type", "unknown"),
                "tasks": status.get("tasks", []),
            }
        except Exception as e:
            context.log.error(f"Error getting connector status: {e}")
            raise
    
    return connector_asset
