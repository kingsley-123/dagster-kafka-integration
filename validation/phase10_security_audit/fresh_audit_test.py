# Save as: validation\phase10_security\fresh_audit_test.py
import sys
import os
import importlib

# Force clear all dagster_kafka modules from cache
modules_to_clear = [key for key in sys.modules.keys() if key.startswith('dagster_kafka')]
for module in modules_to_clear:
    del sys.modules[module]

print("=== Fresh Import Credential Security Audit ===")
print("Testing with forced module reload to ensure we're using the updated code...")
print()

# Fresh import after clearing cache
from dagster_kafka import KafkaResource

def test_configuration_validation_fresh():
    """Test configuration validation with fresh imports"""
    print("Testing configuration validation with forced fresh imports...")
    
    malicious_configs = [
        {
            "name": "command_injection",
            "config": {"client.id": "test; rm -rf /"},
            "expected": "REJECTED"
        },
        {
            "name": "sql_injection", 
            "config": {"consumer.group.id": "test'; DROP TABLE users; --"},
            "expected": "REJECTED"
        },
        {
            "name": "path_traversal",
            "config": {"ssl.ca.location": "../../../etc/passwd"},
            "expected": "REJECTED"
        },
        {
            "name": "script_injection",
            "config": {"client.id": "<script>alert('xss')</script>"},
            "expected": "REJECTED"
        },
        {
            "name": "null_byte_injection",
            "config": {"client.id": "test\x00malicious"},
            "expected": "REJECTED"
        }
    ]
    
    validation_results = []
    
    for test_case in malicious_configs:
        try:
            # Attempt to create resource with malicious config
            kafka_resource = KafkaResource(
                bootstrap_servers="localhost:9092",
                additional_config=test_case["config"]
            )
            
            # If we get here, the config was accepted (BAD)
            validation_results.append({
                "test_name": test_case["name"],
                "config": test_case["config"],
                "result": "ACCEPTED",
                "expected": test_case["expected"],
                "security_concern": True  # This is bad - should have been rejected
            })
            
            print(f"  {test_case['name']}: ACCEPTED ❌ (Should have been {test_case['expected']})")
            
        except ValueError as e:
            # Config was rejected (GOOD)
            validation_results.append({
                "test_name": test_case["name"],
                "config": test_case["config"],
                "result": "REJECTED",
                "expected": test_case["expected"],
                "error": str(e),
                "security_concern": False  # This is good
            })
            
            print(f"  {test_case['name']}: REJECTED ✅ ({test_case['expected']})")
            print(f"    Reason: {str(e)[:80]}...")
            
        except Exception as e:
            # Unexpected error
            validation_results.append({
                "test_name": test_case["name"],
                "config": test_case["config"],
                "result": "ERROR",
                "expected": test_case["expected"],
                "error": str(e),
                "security_concern": True  # Errors are concerning
            })
            
            print(f"  {test_case['name']}: ERROR ❌ - {str(e)[:60]}...")
    
    # Analyze results
    security_concerns = [r for r in validation_results if r.get("security_concern", False)]
    
    print()
    print("FRESH AUDIT RESULTS:")
    print(f"  Tests run: {len(validation_results)}")
    print(f"  Security concerns: {len(security_concerns)}")
    
    if len(security_concerns) == 0:
        print("  Overall status: SECURE ✅")
        print("  🎉 SUCCESS: All malicious configs properly rejected with fresh imports!")
        return "SECURE"
    else:
        print("  Overall status: VULNERABLE ❌")
        print("  ⚠️  ISSUE: Some malicious configs were still accepted")
        return "VULNERABLE"

def check_module_source():
    """Check which file the KafkaResource is actually being imported from"""
    import inspect
    
    print("MODULE SOURCE CHECK:")
    try:
        source_file = inspect.getfile(KafkaResource)
        print(f"  KafkaResource imported from: {source_file}")
        
        # Check if it's the right file
        expected_path = os.path.abspath("dagster_kafka\\resources.py")
        actual_path = os.path.abspath(source_file)
        
        if actual_path == expected_path:
            print("  ✅ Using the correct resources.py file")
        else:
            print(f"  ⚠️  ISSUE: Expected {expected_path}")
            print(f"          But using {actual_path}")
            
    except Exception as e:
        print(f"  ERROR: Could not determine source file: {e}")
    
    print()

if __name__ == "__main__":
    print("Checking module import source...")
    check_module_source()
    
    print("Running fresh credential security validation...")
    result = test_configuration_validation_fresh()
    
    print()
    if result == "SECURE":
        print("🎉 CONCLUSION: Security fix is working correctly with fresh imports!")
        print("The original audit may have been using cached modules.")
    else:
        print("❌ CONCLUSION: There may be an issue with the security fix implementation.")