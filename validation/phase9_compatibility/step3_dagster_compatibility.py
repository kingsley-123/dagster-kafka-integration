# Save as: validation\phase9_compatibility\step3_dagster_compatibility.py
import json
import sys
from datetime import datetime
from dagster import __version__ as dagster_version
import os

print("=== Phase 9 Step 3: Dagster Version Compatibility ===")
print("Testing dagster-kafka compatibility with current Dagster version...")
print()

def parse_version(version_string):
    """Parse version string into comparable tuple"""
    try:
        # Handle versions like "1.8.7" or "1.8.7.dev0"
        clean_version = version_string.split('.dev')[0].split('+')[0]
        parts = clean_version.split('.')
        return tuple(int(part) for part in parts[:3])  # Major, minor, patch
    except:
        return (0, 0, 0)

def check_dagster_version_compatibility():
    """Check if current Dagster version is compatible"""
    
    current_version = parse_version(dagster_version)
    
    # Define minimum required Dagster version for your package
    minimum_version = (1, 5, 0)  # Dagster 1.5.0+
    recommended_version = (1, 8, 0)  # Dagster 1.8.0+
    
    is_compatible = current_version >= minimum_version
    is_recommended = current_version >= recommended_version
    
    return {
        "current_version_string": dagster_version,
        "current_version_tuple": current_version,
        "minimum_required": minimum_version,
        "recommended_version": recommended_version,
        "is_compatible": is_compatible,
        "is_recommended": is_recommended,
        "version_status": "COMPATIBLE" if is_compatible else "INCOMPATIBLE"
    }

def test_dagster_core_imports():
    """Test core Dagster imports needed by dagster-kafka"""
    try:
        from dagster import (
            asset, materialize, resource, IOManager, 
            ConfigurableResource, AssetMaterialization
        )
        
        return {
            "core_imports": "PASS",
            "imports": {
                "asset": "SUCCESS",
                "materialize": "SUCCESS",
                "resource": "SUCCESS",
                "IOManager": "SUCCESS",
                "ConfigurableResource": "SUCCESS",
                "AssetMaterialization": "SUCCESS"
            }
        }
        
    except ImportError as e:
        return {
            "core_imports": "FAIL",
            "error": str(e),
            "imports": {
                "asset": "FAILED",
                "materialize": "FAILED",
                "resource": "FAILED",
                "IOManager": "FAILED",
                "ConfigurableResource": "FAILED",
                "AssetMaterialization": "FAILED"
            }
        }

def test_dagster_kafka_integration():
    """Test dagster-kafka integration with current Dagster version"""
    try:
        from dagster import asset, materialize
        from dagster_kafka import KafkaResource, KafkaIOManager
        
        # Test resource creation with current Dagster
        kafka_resource = KafkaResource(
            bootstrap_servers="localhost:9092",
            additional_config={"client.id": "dagster-compat-test"}
        )
        
        # Test IO manager creation
        io_manager = KafkaIOManager(
            kafka_resource=kafka_resource,
            consumer_group_id="dagster-compat-group",
            enable_dlq=False
        )
        
        # Test asset creation (without materialization for speed)
        @asset(io_manager_key="test_io_manager")
        def dagster_compatibility_test_asset():
            return {"test": "dagster_compatibility", "version": dagster_version}
        
        return {
            "integration_test": "PASS",
            "kafka_resource": "SUCCESS",
            "io_manager": "SUCCESS",
            "asset_creation": "SUCCESS"
        }
        
    except Exception as e:
        return {
            "integration_test": "FAIL",
            "error": str(e),
            "kafka_resource": "FAILED",
            "io_manager": "FAILED",
            "asset_creation": "FAILED"
        }

def main():
    """Run Dagster version compatibility tests"""
    
    print("Running Dagster version compatibility tests...")
    
    # Check Dagster version compatibility
    version_check = check_dagster_version_compatibility()
    print("DAGSTER VERSION CHECK:")
    print(f"  Current version: {version_check['current_version_string']}")
    print(f"  Parsed version: {'.'.join(map(str, version_check['current_version_tuple']))}")
    print(f"  Minimum required: {'.'.join(map(str, version_check['minimum_required']))}")
    print(f"  Recommended version: {'.'.join(map(str, version_check['recommended_version']))}")
    print(f"  Compatibility status: {version_check['version_status']}")
    print(f"  Meets minimum: {'YES' if version_check['is_compatible'] else 'NO'}")
    print(f"  Recommended level: {'YES' if version_check['is_recommended'] else 'NO'}")
    print()
    
    # Test core Dagster imports
    import_results = test_dagster_core_imports()
    print("DAGSTER CORE IMPORTS TEST:")
    print(f"  Overall status: {import_results['core_imports']}")
    for import_name, status in import_results['imports'].items():
        print(f"  {import_name}: {status}")
    
    if import_results['core_imports'] == 'FAIL':
        print(f"  Import error: {import_results.get('error', 'Unknown error')}")
    print()
    
    # Test dagster-kafka integration
    integration_results = test_dagster_kafka_integration()
    print("DAGSTER-KAFKA INTEGRATION TEST:")
    print(f"  Overall status: {integration_results['integration_test']}")
    print(f"  KafkaResource: {integration_results['kafka_resource']}")
    print(f"  KafkaIOManager: {integration_results['io_manager']}")
    print(f"  Asset creation: {integration_results['asset_creation']}")
    
    if integration_results['integration_test'] == 'FAIL':
        print(f"  Integration error: {integration_results.get('error', 'Unknown error')}")
    print()
    
    # Overall compatibility assessment
    overall_compatible = (
        version_check['is_compatible'] and
        import_results['core_imports'] == 'PASS' and
        integration_results['integration_test'] == 'PASS'
    )
    
    compatibility_result = {
        "test_timestamp": datetime.now().isoformat(),
        "dagster_version_check": version_check,
        "import_results": import_results,
        "integration_results": integration_results,
        "overall_compatible": overall_compatible,
        "compatibility_status": "COMPATIBLE" if overall_compatible else "INCOMPATIBLE"
    }
    
    # Save results
    os.makedirs('validation/phase9_compatibility', exist_ok=True)
    with open('validation/phase9_compatibility/dagster_compatibility.json', 'w') as f:
        json.dump(compatibility_result, f, indent=2)
    
    print("DAGSTER COMPATIBILITY SUMMARY:")
    print(f"  Dagster version: {dagster_version}")
    print(f"  Compatibility status: {compatibility_result['compatibility_status']}")
    
    if overall_compatible:
        print("  ✅ PASS: dagster-kafka is compatible with this Dagster version")
    else:
        print("  ❌ FAIL: dagster-kafka has compatibility issues with this Dagster version")
    
    print()
    print("Results saved to: validation/phase9_compatibility/dagster_compatibility.json")
    
    return overall_compatible

if __name__ == "__main__":
    main()