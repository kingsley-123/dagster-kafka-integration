# Save as: validation\phase9_compatibility\step2_python_compatibility.py
import sys
import json
import subprocess
import os
from datetime import datetime

print("=== Phase 9 Step 2: Python Version Compatibility ===")
print("Testing dagster-kafka package compatibility across Python versions...")
print()

def get_current_python_info():
    """Get current Python version information"""
    return {
        "version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
        "major": sys.version_info.major,
        "minor": sys.version_info.minor,
        "micro": sys.version_info.micro,
        "executable": sys.executable
    }

def test_package_import():
    """Test if package imports work in current Python version"""
    try:
        # Test core imports
        from dagster_kafka import KafkaResource, KafkaIOManager
        from dagster import asset, materialize
        import kafka
        
        return {
            "import_test": "PASS",
            "imports": {
                "dagster_kafka": "SUCCESS",
                "dagster": "SUCCESS", 
                "kafka": "SUCCESS"
            }
        }
        
    except ImportError as e:
        return {
            "import_test": "FAIL",
            "error": str(e),
            "imports": {
                "dagster_kafka": "FAILED",
                "dagster": "UNKNOWN",
                "kafka": "UNKNOWN"
            }
        }

def test_basic_resource_creation():
    """Test basic resource creation in current Python version"""
    try:
        from dagster_kafka import KafkaResource, KafkaIOManager
        
        # Test KafkaResource
        kafka_resource = KafkaResource(
            bootstrap_servers="localhost:9092",
            additional_config={"client.id": f"python-compat-test-{sys.version_info.major}-{sys.version_info.minor}"}
        )
        
        # Test KafkaIOManager
        io_manager = KafkaIOManager(
            kafka_resource=kafka_resource,
            consumer_group_id=f"python-compat-group-{sys.version_info.major}-{sys.version_info.minor}",
            enable_dlq=False
        )
        
        return {
            "resource_creation": "PASS",
            "kafka_resource": "SUCCESS",
            "io_manager": "SUCCESS"
        }
        
    except Exception as e:
        return {
            "resource_creation": "FAIL",
            "error": str(e),
            "kafka_resource": "FAILED",
            "io_manager": "FAILED"
        }

def check_python_version_support():
    """Check if current Python version is in supported range"""
    current = sys.version_info
    
    # Define supported version ranges based on your package requirements
    supported_versions = {
        "minimum": (3, 9),
        "maximum": (3, 13),
        "tested": [(3, 9), (3, 10), (3, 11), (3, 12), (3, 13)]
    }
    
    current_tuple = (current.major, current.minor)
    
    is_supported = (supported_versions["minimum"] <= current_tuple <= supported_versions["maximum"])
    is_tested = current_tuple in supported_versions["tested"]
    
    return {
        "current_version": current_tuple,
        "is_supported": is_supported,
        "is_tested": is_tested,
        "supported_range": f"{supported_versions['minimum'][0]}.{supported_versions['minimum'][1]}+ to {supported_versions['maximum'][0]}.{supported_versions['maximum'][1]}",
        "support_status": "SUPPORTED" if is_supported else "UNSUPPORTED"
    }

def main():
    """Run Python version compatibility tests"""
    
    print("Running Python version compatibility tests...")
    
    # Get current Python info
    python_info = get_current_python_info()
    print(f"Current Python version: {python_info['version']}")
    print()
    
    # Check version support
    version_support = check_python_version_support()
    print("VERSION SUPPORT CHECK:")
    print(f"  Current version: Python {version_support['current_version'][0]}.{version_support['current_version'][1]}")
    print(f"  Supported range: Python {version_support['supported_range']}")
    print(f"  Support status: {version_support['support_status']}")
    print(f"  Tested version: {'YES' if version_support['is_tested'] else 'NO'}")
    print()
    
    # Test imports
    import_results = test_package_import()
    print("IMPORT COMPATIBILITY TEST:")
    print(f"  Overall status: {import_results['import_test']}")
    print(f"  dagster-kafka import: {import_results['imports']['dagster_kafka']}")
    print(f"  dagster import: {import_results['imports']['dagster']}")
    print(f"  kafka-python import: {import_results['imports']['kafka']}")
    
    if import_results['import_test'] == 'FAIL':
        print(f"  Import error: {import_results.get('error', 'Unknown error')}")
    print()
    
    # Test resource creation
    resource_results = test_basic_resource_creation()
    print("RESOURCE CREATION TEST:")
    print(f"  Overall status: {resource_results['resource_creation']}")
    print(f"  KafkaResource creation: {resource_results['kafka_resource']}")
    print(f"  KafkaIOManager creation: {resource_results['io_manager']}")
    
    if resource_results['resource_creation'] == 'FAIL':
        print(f"  Creation error: {resource_results.get('error', 'Unknown error')}")
    print()
    
    # Overall compatibility assessment
    overall_compatible = (
        version_support['is_supported'] and
        import_results['import_test'] == 'PASS' and
        resource_results['resource_creation'] == 'PASS'
    )
    
    compatibility_result = {
        "test_timestamp": datetime.now().isoformat(),
        "python_info": python_info,
        "version_support": version_support,
        "import_results": import_results,
        "resource_results": resource_results,
        "overall_compatible": overall_compatible,
        "compatibility_status": "COMPATIBLE" if overall_compatible else "INCOMPATIBLE"
    }
    
    # Save results
    with open('validation/phase9_compatibility/python_compatibility.json', 'w') as f:
        json.dump(compatibility_result, f, indent=2)
    
    print("PYTHON COMPATIBILITY SUMMARY:")
    print(f"  Python version: {python_info['version']}")
    print(f"  Compatibility status: {compatibility_result['compatibility_status']}")
    
    if overall_compatible:
        print("  ✅ PASS: dagster-kafka is compatible with this Python version")
    else:
        print("  ❌ FAIL: dagster-kafka has compatibility issues with this Python version")
    
    print()
    print("Results saved to: validation/phase9_compatibility/python_compatibility.json")
    
    return overall_compatible

if __name__ == "__main__":
    main()