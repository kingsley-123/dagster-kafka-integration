from typing import Dict, List, Any, Optional
from dagster import ConfigurableResource
from pydantic import Field

from dagster_kafka.connect.client import ConfluentConnectClient, ConfluentConnectError


class ConfluentConnectResource(ConfigurableResource):
    """Resource for managing Confluent Connect connectors."""
    
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
        """Set up the resource for execution."""
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
        """List all connector names."""
        return self._client.list_connectors()
    
    def get_connector_info(self, name: str) -> Dict[str, Any]:
        """Get detailed information about a connector."""
        return self._client.get_connector_info(name)
    
    def get_connector_config(self, name: str) -> Dict[str, Any]:
        """Get connector configuration."""
        return self._client.get_connector_config(name)
    
    def get_connector_status(self, name: str) -> Dict[str, Any]:
        """Get connector status."""
        return self._client.get_connector_status(name)
    
    def create_connector(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new connector."""
        return self._client.create_connector(config)
    
    def update_connector_config(
        self, 
        name: str, 
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Update a connector's configuration."""
        return self._client.update_connector_config(name, config)
    
    def delete_connector(self, name: str) -> None:
        """Delete a connector."""
        self._client.delete_connector(name)
    
    def pause_connector(self, name: str) -> None:
        """Pause a connector."""
        self._client.pause_connector(name)
    
    def resume_connector(self, name: str) -> None:
        """Resume a connector."""
        self._client.resume_connector(name)
    
    def restart_connector(self, name: str) -> None:
        """Restart a connector."""
        self._client.restart_connector(name)
    
    def get_connector_topics(self, name: str) -> List[str]:
        """Get topics used by the connector."""
        return self._client.get_connector_topics(name)
    
    def get_connector_tasks(self, name: str) -> List[Dict[str, Any]]:
        """Get tasks information for the connector."""
        return self._client.get_connector_tasks(name)
    
    def restart_task(self, name: str, task_id: int) -> None:
        """Restart a specific task of the connector."""
        self._client.restart_task(name, task_id)
    
    def get_connector_plugins(self) -> List[Dict[str, Any]]:
        """Get all installed connector plugins."""
        return self._client.get_connector_plugins()
    
    def validate_config(
        self, 
        plugin_name: str, 
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Validate connector configuration against plugin requirements."""
        return self._client.validate_config(plugin_name, config)
