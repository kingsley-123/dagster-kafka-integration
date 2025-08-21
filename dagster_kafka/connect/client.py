"""
Confluent Connect REST API client for Dagster Kafka integration.

This module provides a client for interacting with the Confluent Connect REST API,
enabling management of connectors, tasks, and configurations. The client handles
authentication, request formatting, and error handling for all Connect API operations.

Typical usage:
    client = ConfluentConnectClient(base_url="http://localhost:8083")
    connectors = client.list_connectors()
"""
from typing import Dict, List, Any, Optional, Union
from urllib.parse import urljoin
import requests


class ConfluentConnectError(Exception):
    """
    Exception raised for Confluent Connect API errors.
    
    This exception captures all errors related to Confluent Connect API requests,
    including connection, authentication, and response parsing errors. It includes
    detailed error information from the Connect API when available.
    
    Attributes:
        message (str): Explanation of the error
        original_exception (Exception, optional): The original exception that was raised
        response (requests.Response, optional): The response object if available
    """
    pass


class ConfluentConnectClient:
    """
    Client for the Confluent Connect REST API.
    
    This client provides methods for all operations supported by the Confluent Connect
    REST API, including connector management, configuration, and status monitoring.
    It handles authentication, request formatting, and error handling.
    
    Attributes:
        base_url (str): Base URL for the Connect REST API
        auth (Dict[str, str], optional): Authentication credentials
        timeout (int): Request timeout in seconds
    
    Examples:
        >>> client = ConfluentConnectClient(base_url="http://localhost:8083")
        >>> connectors = client.list_connectors()
        >>> status = client.get_connector_status("mysql-connector")
    """

    def __init__(
        self, 
        base_url: str,
        auth: Optional[Dict[str, str]] = None,
        timeout: int = 10,
    ):
        """
        Initialize the Confluent Connect client.
        
        Args:
            base_url: Base URL for the Connect REST API (e.g., "http://localhost:8083")
            auth: Optional authentication configuration as a dictionary.
                 Format: {"username": "user", "password": "pass"}
            timeout: Request timeout in seconds (default: 10)
        
        Note:
            The base_url should point to the Connect REST API endpoint, which is
            typically running on port 8083 by default.
        """
        self.base_url = base_url if base_url.endswith("/") else base_url + "/"
        self.auth = auth
        self.timeout = timeout
        self._session = self._create_session()
        
    def _create_session(self) -> requests.Session:
        """
        Create and configure a requests session.
        
        Creates a new HTTP session with appropriate authentication and headers
        for all requests to the Connect API.
        
        Returns:
            A configured requests.Session object
        """
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
        """
        Make a request to the Connect REST API.
        
        This is a low-level method that handles HTTP requests to the Connect API,
        including error handling and response parsing.
        
        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint (without base URL)
            data: Optional request payload
            params: Optional query parameters
            
        Returns:
            Response data as parsed JSON, or None if no response body
            
        Raises:
            ConfluentConnectError: If the request fails for any reason
        """
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
            # Handle request errors (connection, timeout, etc.)
            error_msg = f"Request failed: {str(e)}"
            
            # Include response details if available
            if hasattr(e, "response") and e.response is not None:
                try:
                    error_details = e.response.json()
                    error_msg = f"{error_msg} - {error_details}"
                except ValueError:
                    error_msg = f"{error_msg} - {e.response.text}"
                    
            raise ConfluentConnectError(error_msg) from e
    
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
            >>> client.list_connectors()
            ['mysql-source', 'elasticsearch-sink']
        """
        return self._request("GET", "connectors")
    
    def get_connector_info(self, name: str) -> Dict[str, Any]:
        """
        Get detailed information about a connector.
        
        Retrieves comprehensive information about a specific connector, including
        its configuration, tasks, and type.
        
        Args:
            name: Connector name
            
        Returns:
            Dictionary containing connector information with keys such as
            'name', 'config', 'tasks', and 'type'
            
        Raises:
            ConfluentConnectError: If the request fails or the connector doesn't exist
            
        Examples:
            >>> client.get_connector_info("mysql-source")
            {'name': 'mysql-source', 'config': {...}, 'tasks': [...], 'type': 'source'}
        """
        return self._request("GET", f"connectors/{name}")
    
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
            
        Examples:
            >>> client.get_connector_config("mysql-source")
            {'connector.class': 'io.debezium.connector.mysql.MySqlConnector', 'tasks.max': '1', ...}
        """
        return self._request("GET", f"connectors/{name}/config")
    
    def get_connector_status(self, name: str) -> Dict[str, Any]:
        """
        Get connector status.
        
        Retrieves the current operational status of a connector and its tasks.
        
        Args:
            name: Connector name
            
        Returns:
            Dictionary containing the connector's status with keys such as
            'name', 'connector', 'tasks', and 'type'
            
        Raises:
            ConfluentConnectError: If the request fails or the connector doesn't exist
            
        Examples:
            >>> client.get_connector_status("mysql-source")
            {
                'name': 'mysql-source',
                'connector': {'state': 'RUNNING', 'worker_id': 'connect:8083'},
                'tasks': [{'id': 0, 'state': 'RUNNING', 'worker_id': 'connect:8083'}],
                'type': 'source'
            }
        """
        return self._request("GET", f"connectors/{name}/status")
    
    def create_connector(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a new connector.
        
        Creates a new connector with the provided configuration. The configuration
        must include a 'name' field to identify the connector.
        
        Args:
            config: Connector configuration. Can be provided in two formats:
                   1. {"name": "my-connector", "config": {...}}
                   2. {"name": "my-connector", "connector.class": "...", ...}
            
        Returns:
            Dictionary containing information about the created connector
            
        Raises:
            ConfluentConnectError: If the request fails or the configuration is invalid
            
        Examples:
            >>> config = {
            ...     "name": "file-source",
            ...     "config": {
            ...         "connector.class": "FileStreamSource",
            ...         "file": "/tmp/test.txt",
            ...         "topic": "test-topic"
            ...     }
            ... }
            >>> client.create_connector(config)
            {'name': 'file-source', 'config': {...}, 'tasks': [], 'type': 'source'}
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
        """
        Update a connector's configuration.
        
        Updates the configuration of an existing connector. The connector will
        be restarted with the new configuration.
        
        Args:
            name: Connector name
            config: New connector configuration (without the 'name' field)
            
        Returns:
            Dictionary containing the updated configuration
            
        Raises:
            ConfluentConnectError: If the request fails, the connector doesn't exist,
                                  or the configuration is invalid
            
        Examples:
            >>> new_config = {
            ...     "connector.class": "FileStreamSource",
            ...     "file": "/tmp/updated.txt",
            ...     "topic": "test-topic"
            ... }
            >>> client.update_connector_config("file-source", new_config)
            {'connector.class': 'FileStreamSource', 'file': '/tmp/updated.txt', ...}
        """
        return self._request("PUT", f"connectors/{name}/config", data=config)
    
    def delete_connector(self, name: str) -> None:
        """
        Delete a connector.
        
        Deletes a connector and all its tasks. This operation cannot be undone.
        
        Args:
            name: Connector name
            
        Raises:
            ConfluentConnectError: If the request fails or the connector doesn't exist
            
        Examples:
            >>> client.delete_connector("file-source")
        """
        self._request("DELETE", f"connectors/{name}")
    
    def pause_connector(self, name: str) -> None:
        """
        Pause a connector.
        
        Pauses a connector and all its tasks, which stops data processing
        but maintains offsets and configuration.
        
        Args:
            name: Connector name
            
        Raises:
            ConfluentConnectError: If the request fails or the connector doesn't exist
            
        Examples:
            >>> client.pause_connector("mysql-source")
        """
        self._request("PUT", f"connectors/{name}/pause")
    
    def resume_connector(self, name: str) -> None:
        """
        Resume a connector.
        
        Resumes a paused connector and all its tasks.
        
        Args:
            name: Connector name
            
        Raises:
            ConfluentConnectError: If the request fails or the connector doesn't exist
            
        Examples:
            >>> client.resume_connector("mysql-source")
        """
        self._request("PUT", f"connectors/{name}/resume")
    
    def restart_connector(self, name: str) -> None:
        """
        Restart a connector.
        
        Restarts a connector and all its tasks. This operation may cause 
        a brief interruption in data processing.
        
        Args:
            name: Connector name
            
        Raises:
            ConfluentConnectError: If the request fails or the connector doesn't exist
            
        Examples:
            >>> client.restart_connector("mysql-source")
        """
        self._request("POST", f"connectors/{name}/restart")
    
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
            
        Examples:
            >>> client.get_connector_topics("mysql-source")
            ['mysql-users', 'mysql-orders']
        """
        return self._request("GET", f"connectors/{name}/topics")
    
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
            
        Examples:
            >>> client.get_connector_tasks("mysql-source")
            [{'id': 0, 'state': 'RUNNING', 'worker_id': 'connect:8083'}]
        """
        return self._request("GET", f"connectors/{name}/tasks")
    
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
            
        Examples:
            >>> client.restart_task("mysql-source", 0)
        """
        self._request("POST", f"connectors/{name}/tasks/{task_id}/restart")
    
    def get_connector_plugins(self) -> List[Dict[str, Any]]:
        """
        Get all installed connector plugins.
        
        Retrieves information about all connector plugins available in the Connect cluster.
        
        Returns:
            List of dictionaries containing plugin information
            
        Raises:
            ConfluentConnectError: If the request fails
            
        Examples:
            >>> client.get_connector_plugins()
            [
                {
                    'class': 'io.debezium.connector.mysql.MySqlConnector',
                    'type': 'source',
                    'version': '1.9.6.Final'
                },
                ...
            ]
        """
        return self._request("GET", "connector-plugins")
    
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
            
        Examples:
            >>> config = {
            ...     "connector.class": "FileStreamSource",
            ...     "file": "/tmp/test.txt",
            ...     "topic": "test-topic"
            ... }
            >>> client.validate_config("org.apache.kafka.connect.file.FileStreamSourceConnector", config)
            {'name': 'test-topic', 'error_count': 0, 'configs': [...]}
        """
        return self._request(
            "PUT", 
            f"connector-plugins/{plugin_name}/config/validate", 
            data={"config": config}
        )