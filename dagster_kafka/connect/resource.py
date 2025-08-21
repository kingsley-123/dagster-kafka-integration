"""
Dagster resource for Confluent Connect integration.

This module provides a ConfigurableResource that wraps the ConfluentConnectClient,
making it available within Dagster pipelines for managing Kafka Connect connectors.
"""
from typing import Dict, List, Any, Optional
from dagster import ConfigurableResource
from pydantic import Field

from dagster_kafka.connect.client import ConfluentConnectClient, ConfluentConnectError


class ConfluentConnectResource(ConfigurableResource):
    """
    Dagster resource for managing Confluent Connect connectors.
    
    This resource provides access to a Confluent Connect REST API client within
    Dagster pipelines, enabling connector management and monitoring operations.
    
    Attributes:
        connect_url (str): URL for the Confluent Connect REST API
        username (str, optional): Username for Connect API authentication
        password (str, optional): Password for Connect API authentication
        timeout (int): Request timeout in seconds
    
    Examples:
        Define the resource in your Dagster definitions:
        
        >>> from dagster import Definitions
        >>> from dagster_kafka.connect import ConfluentConnectResource
        >>>
        >>> defs = Definitions(
        ...     resources={
        ...         "connect": ConfluentConnectResource(
        ...             connect_url="http://localhost:8083",
        ...             username="admin",
        ...             password="password",
        ...             timeout=15,
        ...         )
        ...     }
        ... )
        
        Use the resource in an op:
        
        >>> @op
        ... def manage_connectors(context):
        ...     connect = context.resources.connect
        ...     connectors = connect.list_connectors()
        ...     return connectors
    """
    
    connect_url: str = Field(
        description="URL for the Confluent Connect REST API",
    )
    
    username: Optional[str] = Field(
        default="",
        description="Username for Connect API authentication. Leave empty for no auth.",
    )
    
    password: Optional[str] = Field(
        default="",
        description="Password for Connect API authentication. Leave empty for no auth.",
    )
    
    timeout: int = Field(
        default=10,
        description="Request timeout in seconds",
    )
    
    def setup_for_execution(self, context) -> "ConfluentConnectResource":
        """
        Set up the resource for execution.
        
        Initializes the ConfluentConnectClient when the resource is created.
        
        Args:
            context: The Dagster execution context
            
        Returns:
            The configured resource instance
        """
        # Initialize the client when the resource is created
        auth = None
        if self.username and self.password:  # Check for non-empty strings
            auth = {"username": self.username, "password": self.password}
            
        self._client = ConfluentConnectClient(
            base_url=self.connect_url,
            auth=auth,
            timeout=self.timeout,
        )
        return self
    
    # Expose client methods with Dagster context
    
    def list_connectors(self) -> List[str]:
        """
        List all connector names.
        
        Retrieves the names of all connectors currently registered with 
        the Connect cluster.
        
        Returns:
            List of connector names as strings
            
        Raises:
            ConfluentConnectError: If the request fails
            
        Examples:
            >>> context.resources.connect.list_connectors()
            ['mysql-source', 'elasticsearch-sink']
        """
        return self._client.list_connectors()
    
    def get_connector_info(self, name: str) -> Dict[str, Any]:
        """
        Get detailed information about a connector.
        
        Retrieves comprehensive information about a specific connector, including
        its configuration, tasks, and type.
        
        Args:
            name: Connector name
            
        Returns:
            Dictionary containing connector information
            
        Raises:
            ConfluentConnectError: If the request fails or the connector doesn't exist
        """
        return self._client.get_connector_info(name)
    
    def get_connector_config(self, name: str) -> Dict[str, Any]:
        """
        Get connector configuration.
        
        Retrieves the current configuration for a specific connector.
        
        Args:
            name: Connector name
            
        Returns:
            Dictionary containing the connector's configuration
            
        Raises:
            ConfluentConnectError: If the request fails or the connector doesn't exist
        """
        return self._client.get_connector_config(name)
    
    def get_connector_status(self, name: str) -> Dict[str, Any]:
        """
        Get connector status.
        
        Retrieves the current operational status of a connector and its tasks.
        
        Args:
            name: Connector name
            
        Returns:
            Dictionary containing the connector's status
            
        Raises:
            ConfluentConnectError: If the request fails or the connector doesn't exist
        """
        return self._client.get_connector_status(name)
    
    def create_connector(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a new connector.
        
        Creates a new connector with the provided configuration.
        
        Args:
            config: Connector configuration
            
        Returns:
            Dictionary containing information about the created connector
            
        Raises:
            ConfluentConnectError: If the request fails or the configuration is invalid
        """
        return self._client.create_connector(config)
    
    def update_connector_config(
        self, 
        name: str, 
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Update a connector's configuration.
        
        Updates the configuration of an existing connector. The connector will
        be restarted with the new configuration.
        
        Args:
            name: Connector name
            config: New connector configuration
            
        Returns:
            Dictionary containing the updated configuration
            
        Raises:
            ConfluentConnectError: If the request fails, the connector doesn't exist,
                                  or the configuration is invalid
        """
        return self._client.update_connector_config(name, config)
    
    def delete_connector(self, name: str) -> None:
        """
        Delete a connector.
        
        Deletes a connector and all its tasks. This operation cannot be undone.
        
        Args:
            name: Connector name
            
        Raises:
            ConfluentConnectError: If the request fails or the connector doesn't exist
        """
        self._client.delete_connector(name)
    
    def pause_connector(self, name: str) -> None:
        """
        Pause a connector.
        
        Pauses a connector and all its tasks, which stops data processing
        but maintains offsets and configuration.
        
        Args:
            name: Connector name
            
        Raises:
            ConfluentConnectError: If the request fails or the connector doesn't exist
        """
        self._client.pause_connector(name)
    
    def resume_connector(self, name: str) -> None:
        """
        Resume a connector.
        
        Resumes a paused connector and all its tasks.
        
        Args:
            name: Connector name
            
        Raises:
            ConfluentConnectError: If the request fails or the connector doesn't exist
        """
        self._client.resume_connector(name)
    
    def restart_connector(self, name: str) -> None:
        """
        Restart a connector.
        
        Restarts a connector and all its tasks. This operation may cause 
        a brief interruption in data processing.
        
        Args:
            name: Connector name
            
        Raises:
            ConfluentConnectError: If the request fails or the connector doesn't exist
        """
        self._client.restart_connector(name)
    
    def get_connector_topics(self, name: str) -> List[str]:
        """
        Get topics used by the connector.
        
        Retrieves the list of Kafka topics used by a specific connector.
        
        Args:
            name: Connector name
            
        Returns:
            List of topic names
            
        Raises:
            ConfluentConnectError: If the request fails or the connector doesn't exist
        """
        return self._client.get_connector_topics(name)
    
    def get_connector_tasks(self, name: str) -> List[Dict[str, Any]]:
        """
        Get tasks information for the connector.
        
        Retrieves detailed information about all tasks for a specific connector.
        
        Args:
            name: Connector name
            
        Returns:
            List of dictionaries containing task information
            
        Raises:
            ConfluentConnectError: If the request fails or the connector doesn't exist
        """
        return self._client.get_connector_tasks(name)
    
    def restart_task(self, name: str, task_id: int) -> None:
        """
        Restart a specific task of the connector.
        
        Restarts a single task within a connector. This can be useful for
        recovering from task-specific failures without restarting the entire connector.
        
        Args:
            name: Connector name
            task_id: Task ID to restart
            
        Raises:
            ConfluentConnectError: If the request fails, the connector doesn't exist,
                                  or the task doesn't exist
        """
        self._client.restart_task(name, task_id)
    
    def get_connector_plugins(self) -> List[Dict[str, Any]]:
        """
        Get all installed connector plugins.
        
        Retrieves information about all connector plugins available in the Connect cluster.
        
        Returns:
            List of dictionaries containing plugin information
            
        Raises:
            ConfluentConnectError: If the request fails
        """
        return self._client.get_connector_plugins()
    
    def validate_config(
        self, 
        plugin_name: str, 
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Validate connector configuration against plugin requirements.
        
        Validates a connector configuration against the requirements of a specific
        connector plugin. This can be used to check if a configuration is valid
        before attempting to create or update a connector.
        
        Args:
            plugin_name: Fully qualified name of the connector plugin class
            config: Connector configuration to validate
            
        Returns:
            Dictionary containing validation results
            
        Raises:
            ConfluentConnectError: If the request fails or the plugin doesn't exist
        """
        return self._client.validate_config(plugin_name, config)