"""
Command-line interface for managing Kafka Connect connectors.

This module provides a command-line tool for interacting with the Confluent Connect
REST API, enabling connector management from the command line.
"""
import argparse
import json
import sys
import time
from typing import Dict, Any

from dagster_kafka.connect.client import ConfluentConnectClient, ConfluentConnectError

def print_connector_info(connector_info: Dict[str, Any]):
    """Pretty print connector information."""
    print(f"Name: {connector_info.get('name')}")
    print(f"Type: {connector_info.get('type')}")
    
    if "config" in connector_info:
        print("Configuration:")
        for key, value in connector_info["config"].items():
            print(f"  {key}: {value}")
    
    if "tasks" in connector_info:
        print(f"Tasks: {len(connector_info['tasks'])}")
        for task in connector_info["tasks"]:
            print(f"  Task {task.get('id')}: {task.get('state')}")

def print_connector_status(status: Dict[str, Any]):
    """Pretty print connector status."""
    print(f"Name: {status.get('name')}")
    print(f"Type: {status.get('type', 'unknown')}")
    
    connector = status.get("connector", {})
    print(f"State: {connector.get('state')}")
    print(f"Worker ID: {connector.get('worker_id')}")
    
    tasks = status.get("tasks", [])
    print(f"Tasks: {len(tasks)}")
    for task in tasks:
        print(f"  Task {task.get('id')}: {task.get('state')}")

def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(description='Manage Kafka Connect connectors')
    parser.add_argument('--url', default='http://localhost:8083', help='Kafka Connect REST API URL')
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # List connectors
    list_parser = subparsers.add_parser('list', help='List all connectors')
    
    # Get connector info
    get_parser = subparsers.add_parser('get', help='Get connector details')
    get_parser.add_argument('name', help='Connector name')
    
    # Get connector status
    status_parser = subparsers.add_parser('status', help='Get connector status')
    status_parser.add_argument('name', help='Connector name')
    
    # Create connector
    create_parser = subparsers.add_parser('create', help='Create a new connector')
    create_parser.add_argument('config', help='Connector configuration JSON file')
    
    # Update connector
    update_parser = subparsers.add_parser('update', help='Update connector configuration')
    update_parser.add_argument('name', help='Connector name')
    update_parser.add_argument('config', help='Connector configuration JSON file')
    
    # Delete connector
    delete_parser = subparsers.add_parser('delete', help='Delete a connector')
    delete_parser.add_argument('name', help='Connector name')
    
    # Restart connector
    restart_parser = subparsers.add_parser('restart', help='Restart a connector')
    restart_parser.add_argument('name', help='Connector name')
    
    # Pause connector
    pause_parser = subparsers.add_parser('pause', help='Pause a connector')
    pause_parser.add_argument('name', help='Connector name')
    
    # Resume connector
    resume_parser = subparsers.add_parser('resume', help='Resume a connector')
    resume_parser.add_argument('name', help='Connector name')
    
    # List plugins
    plugins_parser = subparsers.add_parser('plugins', help='List available connector plugins')
    
    args = parser.parse_args()
    
    # Create client
    client = ConfluentConnectClient(base_url=args.url)
    
    try:
        # Execute command
        if args.command == 'list':
            connectors = client.list_connectors()
            print(f"Found {len(connectors)} connectors:")
            for connector in connectors:
                print(f"- {connector}")
        
        elif args.command == 'get':
            info = client.get_connector_info(args.name)
            print_connector_info(info)
        
        elif args.command == 'status':
            status = client.get_connector_status(args.name)
            print_connector_status(status)
        
        elif args.command == 'create':
            with open(args.config, 'r') as f:
                config = json.load(f)
            
            result = client.create_connector(config)
            print("Connector created successfully:")
            print_connector_info(result)
        
        elif args.command == 'update':
            with open(args.config, 'r') as f:
                config = json.load(f)
            
            result = client.update_connector_config(args.name, config)
            print("Connector updated successfully:")
            print_connector_info({"name": args.name, "config": result})
        
        elif args.command == 'delete':
            client.delete_connector(args.name)
            print(f"Connector {args.name} deleted successfully")
        
        elif args.command == 'restart':
            client.restart_connector(args.name)
            print(f"Connector {args.name} restarted successfully")
            
            # Wait a moment for restart to take effect
            time.sleep(2)
            
            # Show updated status
            status = client.get_connector_status(args.name)
            print("Updated status:")
            print_connector_status(status)
        
        elif args.command == 'pause':
            client.pause_connector(args.name)
            print(f"Connector {args.name} paused successfully")
            
            # Wait a moment for pause to take effect
            time.sleep(2)
            
            # Show updated status
            status = client.get_connector_status(args.name)
            print("Updated status:")
            print_connector_status(status)
        
        elif args.command == 'resume':
            client.resume_connector(args.name)
            print(f"Connector {args.name} resumed successfully")
            
            # Wait a moment for resume to take effect
            time.sleep(2)
            
            # Show updated status
            status = client.get_connector_status(args.name)
            print("Updated status:")
            print_connector_status(status)
        
        elif args.command == 'plugins':
            plugins = client.get_connector_plugins()
            print(f"Found {len(plugins)} connector plugins:")
            for plugin in plugins:
                print(f"- Class: {plugin.get('class')}")
                print(f"  Type: {plugin.get('type')}")
                print(f"  Version: {plugin.get('version')}")
                print()
        
        else:
            parser.print_help()
    
    except ConfluentConnectError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()