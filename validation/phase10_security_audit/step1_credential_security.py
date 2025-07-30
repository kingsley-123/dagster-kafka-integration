# Save as: validation\phase10_security\step1_credential_security.py
import json
import os
import tempfile
import logging
import sys
from datetime import datetime
from io import StringIO
from dagster_kafka import KafkaResource, KafkaIOManager
import re

print("=== Phase 10 Step 1: Credential Security Audit ===")
print("Testing credential handling, storage, and exposure prevention...")
print()

class LogCapture:
    """Capture log output to check for credential leakage"""
    def __init__(self):
        self.log_output = StringIO()
        self.handler = logging.StreamHandler(self.log_output)
        self.handler.setLevel(logging.DEBUG)
        
    def start_capture(self):
        """Start capturing logs"""
        logging.getLogger().addHandler(self.handler)
        logging.getLogger().setLevel(logging.DEBUG)
        
    def stop_capture(self):
        """Stop capturing logs and return captured output"""
        logging.getLogger().removeHandler(self.handler)
        return self.log_output.getvalue()

def test_credential_exposure_in_logs():
    """Test if credentials are exposed in log output"""
    try:
        print("Testing credential exposure in logs...")
        
        # Set up log capture
        log_capture = LogCapture()
        log_capture.start_capture()
        
        # Test with sensitive credentials
        test_credentials = {
            "username": "test_user_12345",
            "password": "secret_password_98765", 
            "api_key": "api_key_abcdef123456",
            "token": "bearer_token_xyz789"
        }
        
        # Create resources with credentials
        kafka_resource = KafkaResource(
            bootstrap_servers="localhost:9092",
            additional_config={
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "PLAIN",
                "sasl.username": test_credentials["username"],
                "sasl.password": test_credentials["password"],
                "ssl.ca.location": "/path/to/ca.pem",
                "client.id": "credential-security-test"
            }
        )
        
        io_manager = KafkaIOManager(
            kafka_resource=kafka_resource,
            consumer_group_id="credential-test-group",
            enable_dlq=True
        )
        
        # Stop log capture and analyze
        captured_logs = log_capture.stop_capture()
        
        # Check for credential exposure
        exposures = []
        for cred_name, cred_value in test_credentials.items():
            if cred_value in captured_logs:
                exposures.append({
                    "credential_type": cred_name,
                    "exposed_value": cred_value,
                    "severity": "HIGH"
                })
        
        # Check for partial exposures (first/last few characters)
        sensitive_patterns = [
            r"password['\"]?\s*[:=]\s*['\"]?[\w\-@#$%^&*()]+",
            r"token['\"]?\s*[:=]\s*['\"]?[\w\-]+",
            r"api[_\-]?key['\"]?\s*[:=]\s*['\"]?[\w\-]+",
            r"secret['\"]?\s*[:=]\s*['\"]?[\w\-]+"
        ]
        
        pattern_matches = []
        for pattern in sensitive_patterns:
            matches = re.findall(pattern, captured_logs, re.IGNORECASE)
            if matches:
                pattern_matches.extend(matches)
        
        result = {
            "test_type": "credential_exposure_in_logs",
            "direct_exposures": exposures,
            "pattern_matches": pattern_matches,
            "total_exposures": len(exposures) + len(pattern_matches),
            "logs_captured": len(captured_logs) > 0,
            "security_status": "SECURE" if len(exposures) == 0 and len(pattern_matches) == 0 else "VULNERABLE"
        }
        
        print(f"  Direct credential exposures: {len(exposures)}")
        print(f"  Pattern-based exposures: {len(pattern_matches)}")
        print(f"  Overall status: {result['security_status']}")
        
        if exposures:
            print("  WARNING: Direct credential exposures found!")
            for exp in exposures:
                print(f"    - {exp['credential_type']}: {exp['exposed_value'][:10]}...")
        
        if pattern_matches:
            print("  WARNING: Sensitive patterns found in logs!")
            for match in pattern_matches[:3]:  # Show first 3
                print(f"    - {match[:50]}...")
        
        return result
        
    except Exception as e:
        return {
            "test_type": "credential_exposure_in_logs",
            "error": str(e),
            "security_status": "ERROR"
        }

def test_configuration_validation():
    """Test configuration validation and injection prevention"""
    try:
        print("Testing configuration validation and injection prevention...")
        
        # Test various malicious/invalid configurations
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
                "expected": "HANDLED"
            },
            {
                "name": "script_injection",
                "config": {"client.id": "<script>alert('xss')</script>"},
                "expected": "SANITIZED"
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
                
                # If we get here, the config was accepted
                validation_results.append({
                    "test_name": test_case["name"],
                    "config": test_case["config"],
                    "result": "ACCEPTED",
                    "expected": test_case["expected"],
                    "security_concern": "ACCEPTED" != test_case["expected"]
                })
                
            except Exception as e:
                # Config was rejected (good for security)
                validation_results.append({
                    "test_name": test_case["name"],
                    "config": test_case["config"],
                    "result": "REJECTED",
                    "expected": test_case["expected"],
                    "error": str(e),
                    "security_concern": False
                })
        
        # Analyze results
        security_concerns = [r for r in validation_results if r.get("security_concern", False)]
        
        result = {
            "test_type": "configuration_validation",
            "tests_run": len(validation_results),
            "security_concerns": len(security_concerns),
            "validation_results": validation_results,
            "security_status": "SECURE" if len(security_concerns) == 0 else "VULNERABLE"
        }
        
        print(f"  Validation tests run: {len(validation_results)}")
        print(f"  Security concerns: {len(security_concerns)}")
        print(f"  Overall status: {result['security_status']}")
        
        if security_concerns:
            print("  Security concerns found:")
            for concern in security_concerns:
                print(f"    - {concern['test_name']}: Config accepted when it should be {concern['expected']}")
        
        return result
        
    except Exception as e:
        return {
            "test_type": "configuration_validation",
            "error": str(e),
            "security_status": "ERROR"
        }

def test_error_message_security():
    """Test if error messages leak sensitive information"""
    try:
        print("Testing error message security...")
        
        # Test various error scenarios that might leak info
        error_tests = [
            {
                "name": "invalid_broker_connection",
                "action": lambda: KafkaResource(
                    bootstrap_servers="invalid-broker:9092",
                    additional_config={"sasl.password": "secret123"}
                )
            },
            {
                "name": "invalid_ssl_config",
                "action": lambda: KafkaResource(
                    bootstrap_servers="localhost:9092",
                    additional_config={
                        "security.protocol": "SSL",
                        "ssl.keystore.password": "keystore_secret_456"
                    }
                )
            },
            {
                "name": "invalid_sasl_config",
                "action": lambda: KafkaResource(
                    bootstrap_servers="localhost:9092", 
                    additional_config={
                        "security.protocol": "SASL_PLAINTEXT",
                        "sasl.username": "test_user",
                        "sasl.password": "test_password_789"
                    }
                )
            }
        ]
        
        error_analysis = []
        
        for test in error_tests:
            try:
                # Trigger the error
                test["action"]()
                
                error_analysis.append({
                    "test_name": test["name"],
                    "error_occurred": False,
                    "sensitive_data_leaked": False,
                    "message": "No error occurred"
                })
                
            except Exception as e:
                error_message = str(e)
                
                # Check for sensitive data in error message
                sensitive_patterns = [
                    r"password['\"]?\s*[:=]\s*['\"]?[\w\-@#$%^&*()]+",
                    r"secret[\w]*['\"]?\s*[:=]\s*['\"]?[\w\-]+",
                    r"key[\w]*['\"]?\s*[:=]\s*['\"]?[\w\-]+",
                    r"token['\"]?\s*[:=]\s*['\"]?[\w\-]+"
                ]
                
                leaked_data = []
                for pattern in sensitive_patterns:
                    matches = re.findall(pattern, error_message, re.IGNORECASE)
                    leaked_data.extend(matches)
                
                error_analysis.append({
                    "test_name": test["name"],
                    "error_occurred": True,
                    "sensitive_data_leaked": len(leaked_data) > 0,
                    "leaked_patterns": leaked_data,
                    "message_preview": error_message[:100] + "..."
                })
        
        # Analyze results
        leaky_errors = [e for e in error_analysis if e.get("sensitive_data_leaked", False)]
        
        result = {
            "test_type": "error_message_security",
            "tests_run": len(error_analysis),
            "leaky_errors": len(leaky_errors),
            "error_analysis": error_analysis,
            "security_status": "SECURE" if len(leaky_errors) == 0 else "VULNERABLE"
        }
        
        print(f"  Error tests run: {len(error_analysis)}")
        print(f"  Leaky error messages: {len(leaky_errors)}")
        print(f"  Overall status: {result['security_status']}")
        
        if leaky_errors:
            print("  Sensitive data leakage found in error messages:")
            for leak in leaky_errors:
                print(f"    - {leak['test_name']}: {len(leak['leaked_patterns'])} patterns")
        
        return result
        
    except Exception as e:
        return {
            "test_type": "error_message_security",
            "error": str(e),
            "security_status": "ERROR"
        }

def main():
    """Run comprehensive credential security audit"""
    
    print("Starting Phase 10 Step 1: Credential Security Audit")
    print("This test validates secure credential handling and prevents data leakage")
    print()
    
    # Test 1: Credential exposure in logs
    log_test = test_credential_exposure_in_logs()
    print()
    
    # Test 2: Configuration validation
    config_test = test_configuration_validation()
    print()
    
    # Test 3: Error message security
    error_test = test_error_message_security()
    print()
    
    # Overall security assessment
    all_tests = [log_test, config_test, error_test]
    secure_tests = [t for t in all_tests if t.get("security_status") == "SECURE"]
    vulnerable_tests = [t for t in all_tests if t.get("security_status") == "VULNERABLE"]
    error_tests = [t for t in all_tests if t.get("security_status") == "ERROR"]
    
    overall_security = "SECURE" if len(secure_tests) == len(all_tests) else "VULNERABLE"
    
    security_report = {
        "audit_timestamp": datetime.now().isoformat(),
        "audit_type": "credential_security",
        "tests_performed": {
            "credential_exposure": log_test,
            "configuration_validation": config_test,
            "error_message_security": error_test
        },
        "summary": {
            "total_tests": len(all_tests),
            "secure_tests": len(secure_tests),
            "vulnerable_tests": len(vulnerable_tests),
            "error_tests": len(error_tests),
            "overall_security_status": overall_security
        }
    }
    
    # Save results
    os.makedirs('validation/phase10_security', exist_ok=True)
    with open('validation/phase10_security/credential_security_audit.json', 'w') as f:
        json.dump(security_report, f, indent=2)
    
    print("CREDENTIAL SECURITY AUDIT SUMMARY:")
    print("=" * 50)
    print(f"Tests performed: {len(all_tests)}")
    print(f"Secure tests: {len(secure_tests)}")  
    print(f"Vulnerable tests: {len(vulnerable_tests)}")
    print(f"Error tests: {len(error_tests)}")
    print(f"Overall security status: {overall_security}")
    
    if overall_security == "SECURE":
        print()
        print("✅ PASS: Credential security audit completed - No vulnerabilities found")
    else:
        print()
        print("❌ SECURITY CONCERNS: Vulnerabilities detected in credential handling")
    
    print()
    print("Audit results saved to: validation/phase10_security/credential_security_audit.json")
    
    return overall_security == "SECURE"

if __name__ == "__main__":
    main()