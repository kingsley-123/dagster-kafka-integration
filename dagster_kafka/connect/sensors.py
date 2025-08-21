from typing import List, Dict, Any, Optional
from dagster import sensor, SensorResult, RunRequest, JobDefinition, SensorEvaluationContext
from dagster import Config, asset, materialize
import datetime

class RemediationConfig(Config):
    """Configuration for the connector remediation job."""
    unhealthy_connectors: List[Dict[str, Any]]

def create_connector_health_sensor(
    connector_names: List[str],
    job_def: Optional[JobDefinition] = None,
    minimum_interval_seconds: int = 60,
):
    """
    Create a sensor that monitors the health of Kafka Connect connectors.
    
    Args:
        connector_names: List of connector names to monitor
        job_def: Job to run when unhealthy connectors are detected
        minimum_interval_seconds: Minimum time between sensor runs
        
    Returns:
        A sensor that monitors connector health
    """
    
    @sensor(
        name="connector_health_sensor",
        job=job_def,  # Connect to the remediation job
        minimum_interval_seconds=minimum_interval_seconds,
    )
    def connector_health_sensor(context: SensorEvaluationContext):
        # Get the connect resource
        connect = context.resources.connect
        
        unhealthy_connectors = []
        connector_states = {}
        
        # Check each connector
        for connector_name in connector_names:
            try:
                status = connect.get_connector_status(connector_name)
                connector_state = status["connector"]["state"]
                connector_states[connector_name] = connector_state
                
                # Check connector state
                if connector_state != "RUNNING":
                    context.log.warning(
                        f"Connector {connector_name} is in {connector_state} state"
                    )
                    unhealthy_connectors.append({
                        "name": connector_name,
                        "state": connector_state,
                        "issue": f"Connector state is {connector_state}"
                    })
                
                # Check task states if there are any tasks
                for task in status.get("tasks", []):
                    task_state = task.get("state")
                    task_id = task.get("id")
                    
                    if task_state != "RUNNING":
                        context.log.warning(
                            f"Task {task_id} of connector {connector_name} is in {task_state} state"
                        )
                        unhealthy_connectors.append({
                            "name": connector_name,
                            "task_id": task_id,
                            "state": task_state,
                            "issue": f"Task {task_id} state is {task_state}"
                        })
                        
            except Exception as e:
                context.log.error(
                    f"Error checking connector {connector_name}: {str(e)}"
                )
                unhealthy_connectors.append({
                    "name": connector_name,
                    "issue": f"Error checking status: {str(e)}"
                })
        
        # Log overall status
        context.log.info(
            f"Connector health check completed. "
            f"States: {connector_states}. "
            f"Unhealthy connectors: {len(unhealthy_connectors)}"
        )
        
        # If we have unhealthy connectors and a job to run, request a run
        if unhealthy_connectors and job_def:
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            return RunRequest(
                run_key=f"connector_health_{timestamp}",
                run_config={
                    "ops": {
                        "remediate_unhealthy_connectors": {
                            "config": {
                                "unhealthy_connectors": unhealthy_connectors
                            }
                        }
                    }
                },
            )
        
        # Skip this sensor evaluation if no unhealthy connectors
        return SkipReason(
            f"All connectors healthy" if not unhealthy_connectors 
            else "No remediation job provided"
        )
    
    return connector_health_sensor

# Create a sensor and job together as a bundled solution
def create_connector_health_monitoring(
    connector_names: List[str],
    minimum_interval_seconds: int = 60
):
    """
    Create a complete connector health monitoring solution with a sensor and remediation job.
    
    Args:
        connector_names: List of connector names to monitor
        minimum_interval_seconds: Minimum time between sensor runs
        
    Returns:
        A tuple containing (remediation_job, health_sensor)
    """
    from dagster import job, op
    
    @op
    def remediate_unhealthy_connectors(context, config: RemediationConfig):
        """
        Remediate unhealthy connectors by restarting them.
        """
        connect = context.resources.connect
        unhealthy_connectors = config.unhealthy_connectors
        
        context.log.info(f"Remediating {len(unhealthy_connectors)} unhealthy connectors")
        
        for connector in unhealthy_connectors:
            connector_name = connector["name"]
            issue = connector["issue"]
            
            context.log.info(f"Remediating connector {connector_name}: {issue}")
            
            # If it's a task issue, restart the specific task
            if "task_id" in connector:
                task_id = connector["task_id"]
                context.log.info(f"Restarting task {task_id} of connector {connector_name}")
                try:
                    connect.restart_task(connector_name, int(task_id))
                    context.log.info(f"Successfully restarted task {task_id}")
                except Exception as e:
                    context.log.error(f"Failed to restart task {task_id}: {e}")
            
            # Otherwise restart the entire connector
            else:
                context.log.info(f"Restarting connector {connector_name}")
                try:
                    connect.restart_connector(connector_name)
                    context.log.info(f"Successfully restarted connector {connector_name}")
                except Exception as e:
                    context.log.error(f"Failed to restart connector {connector_name}: {e}")
        
        return {
            "num_connectors_remediated": len(unhealthy_connectors),
            "connector_names": [c["name"] for c in unhealthy_connectors]
        }
    
    @job
    def connector_remediation_job():
        remediate_unhealthy_connectors()
    
    # Create the sensor with our job
    health_sensor = create_connector_health_sensor(
        connector_names=connector_names,
        job_def=connector_remediation_job,
        minimum_interval_seconds=minimum_interval_seconds,
    )
    
    return connector_remediation_job, health_sensor
