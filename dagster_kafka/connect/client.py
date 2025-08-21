import requests
from typing import Dict, List, Any, Optional, Union
from urllib.parse import urljoin


class ConfluentConnectError(Exception):
    """Base exception for Confluent Connect client errors."""
    pass


class ConfluentConnectClient:
    """Client for the Confluent Connect REST API."""

    def __init__(
        self, 
        base_url: str,
        auth: Optional[Dict[str, str]] = None,
        timeout: int = 10,
    ):
        """Initialize the Confluent Connect client."""
        self.base_url = base_url if base_url.endswith("/") else base_url + "/"
        self.auth = auth
        self.timeout = timeout
        self._session = self._create_session()
        
    def _create_session(self) -> requests.Session:
        """Create and configure a requests session."""
        session = requests.Session()
        
        # Configure authentication if provided
        if self.auth and "username" in self.auth and "password" in self.auth:
            session.auth = (self.auth["username"], self.auth["password"])
            
        # Add default headers
        session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
        })
        
        return session
    
    def _request(
        self, 
        method: str, 
        endpoint: str, 
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Make a request to the Connect REST API."""
        url = urljoin(self.base_url, endpoint)
        
        try:
            response = self._session.request(
                method=method,
                url=url,
                json=data,
                params=params,
                timeout=self.timeout,
            )
            
            # Raise for HTTP errors
            response.raise_for_status()
            
            # Return JSON response if available, otherwise return text
            if response.text:
                return response.json()
            return None
            
        except requests.RequestException as e:
            # Handle request errors
            error_msg = f"Request failed: {str(e)}"
            
            # Include response details if available
            if hasattr(e, "response") and e.response is not None:
                try:
                    error_details = e.response.json()
                    error_msg = f"{error_msg} - {error_details}"
                except ValueError:
                    error_msg = f"{error_msg} - {e.response.text}"
                    
            raise ConfluentConnectError(error_msg) from e
    
    # Connector methods
    
    def list_connectors(self) -> List[str]:
        """List all connector names."""
        return self._request("GET", "connectors")
    
    def get_connector_info(self, name: str) -> Dict[str, Any]:
        """Get detailed information about a connector."""
        return self._request("GET", f"connectors/{name}")
    
    def get_connector_config(self, name: str) -> Dict[str, Any]:
        """Get connector configuration."""
        return self._request("GET", f"connectors/{name}/config")
    
    def get_connector_status(self, name: str) -> Dict[str, Any]:
        """Get connector status."""
        return self._request("GET", f"connectors/{name}/status")
    
    def create_connector(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a new connector.
        
        Args:
            config: Either a configuration dict containing name and config fields,
                   or just the connector configuration with name included.
        """
        # Handle both formats for creating connectors
        if "config" in config and "name" in config:
            # Format: {"name": "my-connector", "config": {...}}
            return self._request("POST", "connectors", data=config)
        elif "name" in config:
            # Format: {"name": "my-connector", "connector.class": "...", ...}
            # Convert to {"name": "my-connector", "config": {...}}
            name = config["name"]
            return self._request("POST", "connectors", data={
                "name": name,
                "config": config
            })
        else:
            raise ConfluentConnectError("Connector configuration must include 'name'")
    
    def update_connector_config(
        self, 
        name: str, 
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Update a connector's configuration."""
        return self._request("PUT", f"connectors/{name}/config", data=config)
    
    def delete_connector(self, name: str) -> None:
        """Delete a connector."""
        self._request("DELETE", f"connectors/{name}")
    
    def pause_connector(self, name: str) -> None:
        """Pause a connector."""
        self._request("PUT", f"connectors/{name}/pause")
    
    def resume_connector(self, name: str) -> None:
        """Resume a connector."""
        self._request("PUT", f"connectors/{name}/resume")
    
    def restart_connector(self, name: str) -> None:
        """Restart a connector."""
        self._request("POST", f"connectors/{name}/restart")
    
    def get_connector_topics(self, name: str) -> List[str]:
        """Get topics used by the connector."""
        return self._request("GET", f"connectors/{name}/topics")
    
    def get_connector_tasks(self, name: str) -> List[Dict[str, Any]]:
        """Get tasks information for the connector."""
        return self._request("GET", f"connectors/{name}/tasks")
    
    def restart_task(self, name: str, task_id: int) -> None:
        """Restart a specific task of the connector."""
        self._request("POST", f"connectors/{name}/tasks/{task_id}/restart")
    
    def get_connector_plugins(self) -> List[Dict[str, Any]]:
        """Get all installed connector plugins."""
        return self._request("GET", "connector-plugins")
    
    def validate_config(
        self, 
        plugin_name: str, 
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Validate connector configuration against plugin requirements."""
        return self._request(
            "PUT", 
            f"connector-plugins/{plugin_name}/config/validate", 
            data={"config": config}
        )
