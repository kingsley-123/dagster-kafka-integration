"""
Confluent Connect connector assets for Dagster.

This module provides factory functions for creating Dagster assets that represent
Kafka Connect connectors, enabling connector lifecycle management within the Dagster
asset framework.
"""
from typing import Dict, Any, Optional, List
from dagster import asset, AssetExecutionContext, Config

class ConnectorConfig(Config):
    """
    Configuration for a Kafka Connect connector asset.
    
    This class defines the configuration schema for connector assets,
    ensuring proper validation of connector configurations.
    
    Attributes:
        name (str): The name of the connector
        connector_class (str): The Java class name of the connector
        config (Dict[str, Any]): Additional connector configuration
        
    Examples:
        >>> config = ConnectorConfig(
        ...     name="mysql-source",
        ...     connector_class="io.debezium.connector.mysql.MySqlConnector",
        ...     config={
        ...         "database.hostname": "mysql",
        ...         "database.port": "3306",
        ...         "database.user": "debezium",
        ...         "database.password": "dbz",
        ...         "database.server.id": "184054",
        ...         "database.server.name": "dbserver1",
        ...         "database.include.list": "inventory",
        ...         "database.history.kafka.bootstrap.servers": "kafka:9092",
        ...         "database.history.kafka.topic": "schema-changes.inventory"
        ...     }
        ... )
    """
    
    name: str
    connector_class: str
    config: Dict[str, Any]
    
    def to_connect_config(self) -> Dict[str, Any]:
        """
        Convert to Kafka Connect API format.
        
        Transforms the configuration to the format expected by the Kafka Connect API.
        
        Returns:
            Dictionary in the format expected by the Kafka Connect API
            
        Examples:
            >>> config = ConnectorConfig(
            ...     name="mysql-source",
            ...     connector_class="io.debezium.connector.mysql.MySqlConnector",
            ...     config={"database.hostname": "mysql"}
            ... )
            >>> config.to_connect_config()
            {
                'name': 'mysql-source',
                'config': {
                    'connector.class': 'io.debezium.connector.mysql.MySqlConnector',
                    'name': 'mysql-source',
                    'database.hostname': 'mysql'
                }
            }
        """
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
    
    This function creates a Dagster asset that manages the lifecycle of a Kafka Connect
    connector, handling creation, updates, and status monitoring.
    
    Args:
        resource_key: The resource key for the ConfluentConnectResource (default: "connect")
        group_name: The asset group name (default: "kafka_connect")
        key_prefix: Optional prefix for the asset key
        
    Returns:
        A Dagster asset that manages a Kafka Connect connector
        
    Examples:
        >>> from dagster import Definitions
        >>> from dagster_kafka.connect import ConfluentConnectResource, create_connector_asset
        >>>
        >>> # Create a connector asset
        >>> mysql_connector = create_connector_asset(
        ...     group_name="source_connectors",
        ...     key_prefix=["mysql", "cdc"]
        ... )
        >>>
        >>> # Define your Dagster job with the connector asset
        >>> defs = Definitions(
        ...     assets=[mysql_connector],
        ...     resources={
        ...         "connect": ConfluentConnectResource(
        ...             connect_url="http://localhost:8083",
        ...         )
        ...     },
        ... )
    """
    
    @asset(
        group_name=group_name,
        key_prefix=key_prefix,
        compute_kind="kafka_connect",
        required_resource_keys={resource_key},
    )
    def connector_asset(
        context: AssetExecutionContext,
        config: ConnectorConfig,
    ) -> Dict[str, Any]:
        """
        A Dagster asset representing a Kafka Connect connector.
        
        This asset creates or updates a connector and returns its status.
        
        Args:
            context: The Dagster execution context
            config: Connector configuration
            
        Returns:
            Dictionary containing connector status information
            
        Raises:
            Exception: If connector creation, update, or status check fails
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