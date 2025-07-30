# Save as: validation\phase10_security\test_security_fix.py
import sys
import os

# Add the project root to the path so we can import dagster_kafka
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from dagster_kafka import KafkaResource

print("=== Testing Security Configuration Fix ===")
print("Verifying that malicious configurations are now properly rejected...")
print()

def test_malicious_config_rejection():
    """Test that malicious configurations are properly rejected"""
    
    malicious_configs = [
        {
            "name": "command_injection",
            "config": {"client.id": "test; rm -rf /"},
            "should_fail": True
        },
        {
            "name": "sql_injection", 
            "config": {"consumer.group.id": "test'; DROP TABLE users; --"},
            "should_fail": True
        },
        {
            "name": "path_traversal",
            "config": {"ssl.ca.location": "../../../etc/passwd"},
            "should_fail": True
        },
        {
            "name": "script_injection",
            "config": {"client.id": "<script>alert('xss')</script>"},
            "should_fail": True
        },
        {
            "name": "null_byte_injection",
            "config": {"client.id": "test\x00malicious"},
            "should_fail": True
        },
        {
            "name": "valid_config",
            "config": {"client.id": "valid-client-123"},
            "should_fail": False
        },
        {
            "name": "valid_ssl_config",
            "config": {"ssl.ca.location": "/path/to/ca.pem"},
            "should_fail": False
        }
    ]
    
    results = []
    
    for test_case in malicious_configs:
        try:
            # Attempt to create resource with the config
            kafka_resource = KafkaResource(
                bootstrap_servers="localhost:9092",
                additional_config=test_case["config"]
            )
            
            # If we get here, config was accepted
            results.append({
                "test": test_case["name"],
                "expected_to_fail": test_case["should_fail"],
                "actually_failed": False,
                "result": "PASS" if not test_case["should_fail"] else "FAIL"
            })
            
            status = "✅ EXPECTED" if not test_case["should_fail"] else "❌ SECURITY ISSUE"
            print(f"  {test_case['name']}: CONFIG ACCEPTED ({status})")
            
        except ValueError as e:
            # Config was rejected (good for malicious configs)
            results.append({
                "test": test_case["name"],
                "expected_to_fail": test_case["should_fail"],
                "actually_failed": True,
                "error": str(e),
                "result": "PASS" if test_case["should_fail"] else "FAIL"
            })
            
            status = "✅ EXPECTED" if test_case["should_fail"] else "❌ FALSE POSITIVE"
            print(f"  {test_case['name']}: CONFIG REJECTED ({status})")
            print(f"    Reason: {str(e)[:80]}...")
            
        except Exception as e:
            # Unexpected error
            results.append({
                "test": test_case["name"],
                "expected_to_fail": test_case["should_fail"],
                "actually_failed": True,
                "error": f"Unexpected error: {str(e)}",
                "result": "ERROR"
            })
            
            print(f"  {test_case['name']}: UNEXPECTED ERROR - {str(e)[:60]}...")
    
    # Analyze results
    passed_tests = [r for r in results if r["result"] == "PASS"]
    failed_tests = [r for r in results if r["result"] == "FAIL"]
    error_tests = [r for r in results if r["result"] == "ERROR"]
    
    print()
    print("SECURITY FIX VALIDATION RESULTS:")
    print("=" * 40)
    print(f"  Tests run: {len(results)}")
    print(f"  Tests passed: {len(passed_tests)}")
    print(f"  Tests failed: {len(failed_tests)}")
    print(f"  Tests with errors: {len(error_tests)}")
    
    if len(failed_tests) == 0 and len(error_tests) == 0:
        print()
        print("🎉 SUCCESS: All security validation tests passed!")
        print("✅ Malicious configurations are now properly rejected")
        print("✅ Valid configurations are still accepted")
        return True
    else:
        print()
        print("❌ ISSUES FOUND:")
        if failed_tests:
            print("  Failed tests:")
            for failed in failed_tests:
                expected = "rejection" if failed["expected_to_fail"] else "acceptance"
                print(f"    - {failed['test']}: Expected {expected}")
        
        if error_tests:
            print("  Error tests:")
            for error in error_tests:
                print(f"    - {error['test']}: {error.get('error', 'Unknown error')}")
        
        return False

def test_bootstrap_servers_validation():
    """Test that bootstrap_servers validation works"""
    print("TESTING BOOTSTRAP SERVERS VALIDATION:")
    
    bootstrap_tests = [
        {
            "name": "valid_single_server",
            "servers": "localhost:9092",
            "should_fail": False
        },
        {
            "name": "valid_multiple_servers",
            "servers": "broker1:9092,broker2:9092",
            "should_fail": False
        },
        {
            "name": "command_injection_in_servers",
            "servers": "localhost:9092; rm -rf /",
            "should_fail": True
        },
        {
            "name": "invalid_format",
            "servers": "not-a-valid-format",
            "should_fail": False  # This should actually be accepted as it's just a hostname
        },
        {
            "name": "empty_servers",
            "servers": "",
            "should_fail": True
        }
    ]
    
    for test_case in bootstrap_tests:
        try:
            kafka_resource = KafkaResource(
                bootstrap_servers=test_case["servers"]
            )
            
            status = "✅ EXPECTED" if not test_case["should_fail"] else "❌ ISSUE"
            print(f"  {test_case['name']}: ACCEPTED ({status})")
            
        except ValueError as e:
            status = "✅ EXPECTED" if test_case["should_fail"] else "❌ FALSE POSITIVE"
            print(f"  {test_case['name']}: REJECTED ({status})")
            print(f"    Reason: {str(e)[:60]}...")
    
    print()

if __name__ == "__main__":
    print("Starting security fix validation tests...")
    print()
    
    # Test malicious config rejection
    config_test_passed = test_malicious_config_rejection()
    print()
    
    # Test bootstrap servers validation
    test_bootstrap_servers_validation()
    
    if config_test_passed:
        print("🎉 SECURITY FIX VALIDATED: Your package now properly rejects malicious configurations!")
        print("Ready to re-run Phase 10 security audit.")
    else:
        print("⚠️  ADDITIONAL WORK NEEDED: Some security tests are still failing.")