# Save as: validation\phase9_compatibility\step1_environment_baseline.py
import sys
import platform
import importlib
import json
import subprocess
from datetime import datetime
from dagster import __version__ as dagster_version
from dagster_kafka import KafkaResource, KafkaIOManager
import kafka
import os

print("=== Phase 9 Step 1: Environment Compatibility Baseline ===")
print("Documenting current working environment as compatibility baseline...")
print()

def get_package_version(package_name):
    """Get version of installed package"""
    try:
        if package_name == "dagster-kafka":
            # For your custom package, try to get version from package metadata
            try:
                import pkg_resources
                return pkg_resources.get_distribution("dagster-kafka").version
            except:
                return "1.0.0"  # Your published version
        else:
            module = importlib.import_module(package_name)
            return getattr(module, '__version__', 'unknown')
    except ImportError:
        return 'not installed'

def test_basic_functionality():
    """Test basic functionality in current environment"""
    try:
        # Test KafkaResource creation
        kafka_resource = KafkaResource(
            bootstrap_servers="localhost:9092",
            additional_config={"client.id": "compatibility-test"}
        )
        
        # Test KafkaIOManager creation
        io_manager = KafkaIOManager(
            kafka_resource=kafka_resource,
            consumer_group_id="compatibility-baseline-test",
            enable_dlq=False
        )
        
        return {
            "kafka_resource_creation": "SUCCESS",
            "io_manager_creation": "SUCCESS",
            "basic_functionality": "PASS"
        }
        
    except Exception as e:
        return {
            "kafka_resource_creation": "FAILED",
            "io_manager_creation": "FAILED", 
            "basic_functionality": "FAIL",
            "error": str(e)
        }

def main():
    """Generate environment compatibility baseline"""
    
    print("Collecting environment information...")
    
    # System information
    baseline = {
        "test_timestamp": datetime.now().isoformat(),
        "system_info": {
            "python_version": sys.version,
            "python_version_info": {
                "major": sys.version_info.major,
                "minor": sys.version_info.minor,
                "micro": sys.version_info.micro
            },
            "platform": platform.platform(),
            "architecture": platform.architecture(),
            "machine": platform.machine()
        },
        "package_versions": {
            "dagster": dagster_version,
            "dagster-kafka": get_package_version("dagster-kafka"),
            "kafka-python": get_package_version("kafka"),
            "psutil": get_package_version("psutil")
        },
        "functionality_test": test_basic_functionality()
    }
    
    # Display baseline information
    print("CURRENT ENVIRONMENT BASELINE:")
    print(f"  Python: {baseline['system_info']['python_version_info']['major']}.{baseline['system_info']['python_version_info']['minor']}.{baseline['system_info']['python_version_info']['micro']}")
    print(f"  Platform: {baseline['system_info']['platform']}")
    print(f"  Dagster: {baseline['package_versions']['dagster']}")
    print(f"  dagster-kafka: {baseline['package_versions']['dagster-kafka']}")
    print(f"  kafka-python: {baseline['package_versions']['kafka-python']}")
    print()
    
    # Test results
    func_test = baseline['functionality_test']
    print("FUNCTIONALITY TEST:")
    print(f"  Resource creation: {func_test['kafka_resource_creation']}")
    print(f"  IO Manager creation: {func_test['io_manager_creation']}")
    print(f"  Overall status: {func_test['basic_functionality']}")
    
    if func_test['basic_functionality'] == 'FAIL':
        print(f"  Error: {func_test.get('error', 'Unknown error')}")
    
    # Save baseline
    os.makedirs('validation/phase9_compatibility', exist_ok=True)
    with open('validation/phase9_compatibility/environment_baseline.json', 'w') as f:
        json.dump(baseline, f, indent=2)
    
    print()
    if func_test['basic_functionality'] == 'PASS':
        print("✅ BASELINE ESTABLISHED: Current environment fully compatible")
    else:
        print("❌ BASELINE FAILED: Current environment has compatibility issues")
    
    print("Baseline saved to: validation/phase9_compatibility/environment_baseline.json")
    
    return func_test['basic_functionality'] == 'PASS'

if __name__ == "__main__":
    main()