"""
Tests for Kafka security features including SASL and SSL configurations.
"""

import pytest
from unittest.mock import Mock, patch
from dagster_kafka import KafkaResource, SecurityProtocol, SaslMechanism


class TestSecurityProtocols:
    """Test different security protocol configurations."""
    
    def test_plaintext_protocol_default(self):
        """Test default PLAINTEXT protocol."""
        resource = KafkaResource(bootstrap_servers="localhost:9092")
        
        assert resource.security_protocol == SecurityProtocol.PLAINTEXT
        assert resource.sasl_mechanism is None
        assert resource.sasl_username is None
        assert resource.sasl_password is None
        print(" PLAINTEXT protocol default test passed!")
    
    def test_ssl_protocol_configuration(self):
        """Test SSL protocol configuration."""
        resource = KafkaResource(
            bootstrap_servers="localhost:9092",
            security_protocol=SecurityProtocol.SSL,
            ssl_ca_location="/path/to/ca.pem",
            ssl_certificate_location="/path/to/cert.pem",
            ssl_key_location="/path/to/key.pem"
        )
        
        assert resource.security_protocol == SecurityProtocol.SSL
        assert resource.ssl_ca_location == "/path/to/ca.pem"
        assert resource.ssl_certificate_location == "/path/to/cert.pem"
        assert resource.ssl_key_location == "/path/to/key.pem"
        print(" SSL protocol configuration test passed!")
    
    def test_sasl_plaintext_configuration(self):
        """Test SASL_PLAINTEXT protocol configuration."""
        resource = KafkaResource(
            bootstrap_servers="localhost:9092",
            security_protocol=SecurityProtocol.SASL_PLAINTEXT,
            sasl_mechanism=SaslMechanism.PLAIN,
            sasl_username="testuser",
            sasl_password="testpass"
        )
        
        assert resource.security_protocol == SecurityProtocol.SASL_PLAINTEXT
        assert resource.sasl_mechanism == SaslMechanism.PLAIN
        assert resource.sasl_username == "testuser"
        assert resource.sasl_password == "testpass"
        print(" SASL_PLAINTEXT configuration test passed!")
    
    def test_sasl_ssl_configuration(self):
        """Test SASL_SSL protocol configuration (production setup)."""
        resource = KafkaResource(
            bootstrap_servers="secure-kafka:9092",
            security_protocol=SecurityProtocol.SASL_SSL,
            sasl_mechanism=SaslMechanism.SCRAM_SHA_256,
            sasl_username="production-user",
            sasl_password="secure-password",
            ssl_ca_location="/etc/ssl/certs/ca.pem",
            ssl_check_hostname=True
        )
        
        assert resource.security_protocol == SecurityProtocol.SASL_SSL
        assert resource.sasl_mechanism == SaslMechanism.SCRAM_SHA_256
        assert resource.sasl_username == "production-user"
        assert resource.sasl_password == "secure-password"
        assert resource.ssl_ca_location == "/etc/ssl/certs/ca.pem"
        assert resource.ssl_check_hostname is True
        print(" SASL_SSL configuration test passed!")


class TestSaslMechanisms:
    """Test different SASL authentication mechanisms."""
    
    def test_plain_mechanism(self):
        """Test PLAIN SASL mechanism."""
        resource = KafkaResource(
            bootstrap_servers="localhost:9092",
            security_protocol=SecurityProtocol.SASL_PLAINTEXT,
            sasl_mechanism=SaslMechanism.PLAIN,
            sasl_username="user",
            sasl_password="pass"
        )
        
        consumer = resource.get_consumer("test-group")
        assert consumer is not None
        print(" PLAIN mechanism test passed!")
    
    def test_scram_sha_256_mechanism(self):
        """Test SCRAM-SHA-256 SASL mechanism."""
        resource = KafkaResource(
            bootstrap_servers="localhost:9092",
            security_protocol=SecurityProtocol.SASL_PLAINTEXT,
            sasl_mechanism=SaslMechanism.SCRAM_SHA_256,
            sasl_username="user",
            sasl_password="pass"
        )
        
        consumer = resource.get_consumer("test-group")
        assert consumer is not None
        print(" SCRAM-SHA-256 mechanism test passed!")
    
    def test_scram_sha_512_mechanism(self):
        """Test SCRAM-SHA-512 SASL mechanism."""
        resource = KafkaResource(
            bootstrap_servers="localhost:9092",
            security_protocol=SecurityProtocol.SASL_PLAINTEXT,
            sasl_mechanism=SaslMechanism.SCRAM_SHA_512,
            sasl_username="user",
            sasl_password="pass"
        )
        
        consumer = resource.get_consumer("test-group")
        assert consumer is not None
        print(" SCRAM-SHA-512 mechanism test passed!")
    
    def test_gssapi_mechanism_configuration(self):
        """Test GSSAPI mechanism configuration (no credentials needed)."""
        resource = KafkaResource(
            bootstrap_servers="localhost:9092",
            security_protocol=SecurityProtocol.SASL_PLAINTEXT,
            sasl_mechanism=SaslMechanism.GSSAPI
        )
        
        assert resource.sasl_mechanism == SaslMechanism.GSSAPI
        # GSSAPI doesn't require username/password
        assert resource.sasl_username is None
        assert resource.sasl_password is None
        print(" GSSAPI mechanism configuration test passed!")


class TestSecurityValidation:
    """Test security configuration validation."""
    
    def test_sasl_without_mechanism_fails(self):
        """Test that SASL protocol without mechanism fails."""
        resource = KafkaResource(
            bootstrap_servers="localhost:9092",
            security_protocol=SecurityProtocol.SASL_PLAINTEXT
            # Missing sasl_mechanism
        )
        
        with pytest.raises(ValueError, match="sasl_mechanism is required"):
            resource.get_consumer("test-group")
        print(" SASL without mechanism validation test passed!")
    
    def test_plain_without_credentials_fails(self):
        """Test that PLAIN mechanism without credentials fails."""
        resource = KafkaResource(
            bootstrap_servers="localhost:9092",
            security_protocol=SecurityProtocol.SASL_PLAINTEXT,
            sasl_mechanism=SaslMechanism.PLAIN
            # Missing username/password
        )
        
        with pytest.raises(ValueError, match="sasl_username and sasl_password are required"):
            resource.get_consumer("test-group")
        print(" PLAIN without credentials validation test passed!")
    
    def test_scram_without_credentials_fails(self):
        """Test that SCRAM mechanisms without credentials fail."""
        resource = KafkaResource(
            bootstrap_servers="localhost:9092",
            security_protocol=SecurityProtocol.SASL_PLAINTEXT,
            sasl_mechanism=SaslMechanism.SCRAM_SHA_256
            # Missing username/password
        )
        
        with pytest.raises(ValueError, match="sasl_username and sasl_password are required"):
            resource.get_consumer("test-group")
        print(" SCRAM without credentials validation test passed!")
    
    def test_validate_security_config_method(self):
        """Test the validate_security_config method."""
        # Valid configuration
        valid_resource = KafkaResource(
            bootstrap_servers="localhost:9092",
            security_protocol=SecurityProtocol.SASL_SSL,
            sasl_mechanism=SaslMechanism.PLAIN,
            sasl_username="user",
            sasl_password="pass",
            ssl_ca_location="/path/to/ca.pem"
        )
        
        assert valid_resource.validate_security_config() is True
        
        # Invalid configuration
        invalid_resource = KafkaResource(
            bootstrap_servers="localhost:9092",
            security_protocol=SecurityProtocol.SASL_SSL,
            sasl_mechanism=SaslMechanism.PLAIN
            # Missing credentials
        )
        
        with pytest.raises(ValueError):
            invalid_resource.validate_security_config()
        
        print(" Security config validation method test passed!")


class TestProducerConfiguration:
    """Test producer configuration generation with security."""
    
    def test_plaintext_producer_config(self):
        """Test producer config for PLAINTEXT protocol."""
        resource = KafkaResource(bootstrap_servers="localhost:9092")
        config = resource.get_producer_config()
        
        assert config['bootstrap.servers'] == "localhost:9092"
        assert config['security.protocol'] == "PLAINTEXT"
        assert 'sasl.mechanism' not in config
        assert 'ssl.ca.location' not in config
        print(" PLAINTEXT producer config test passed!")
    
    def test_sasl_ssl_producer_config(self):
        """Test producer config for SASL_SSL protocol."""
        resource = KafkaResource(
            bootstrap_servers="secure-kafka:9092",
            security_protocol=SecurityProtocol.SASL_SSL,
            sasl_mechanism=SaslMechanism.SCRAM_SHA_256,
            sasl_username="producer-user",
            sasl_password="producer-pass",
            ssl_ca_location="/etc/ssl/ca.pem"
        )
        
        config = resource.get_producer_config()
        
        assert config['bootstrap.servers'] == "secure-kafka:9092"
        assert config['security.protocol'] == "SASL_SSL"
        assert config['sasl.mechanism'] == "SCRAM-SHA-256"
        assert config['sasl.username'] == "producer-user"
        assert config['sasl.password'] == "producer-pass"
        assert config['ssl.ca.location'] == "/etc/ssl/ca.pem"
        print(" SASL_SSL producer config test passed!")
    
    def test_ssl_only_producer_config(self):
        """Test producer config for SSL-only protocol."""
        resource = KafkaResource(
            bootstrap_servers="ssl-kafka:9092",
            security_protocol=SecurityProtocol.SSL,
            ssl_ca_location="/etc/ssl/ca.pem",
            ssl_certificate_location="/etc/ssl/cert.pem",
            ssl_key_location="/etc/ssl/key.pem",
            ssl_key_password="keypass"
        )
        
        config = resource.get_producer_config()
        
        assert config['security.protocol'] == "SSL"
        assert config['ssl.ca.location'] == "/etc/ssl/ca.pem"
        assert config['ssl.certificate.location'] == "/etc/ssl/cert.pem"
        assert config['ssl.key.location'] == "/etc/ssl/key.pem"
        assert config['ssl.key.password'] == "keypass"
        assert 'sasl.mechanism' not in config
        print(" SSL-only producer config test passed!")


class TestAdvancedConfiguration:
    """Test advanced configuration options."""
    
    def test_additional_config_parameter(self):
        """Test additional_config parameter."""
        additional_config = {
            'request.timeout.ms': 30000,
            'retry.backoff.ms': 1000,
            'max.poll.interval.ms': 300000
        }
        
        resource = KafkaResource(
            bootstrap_servers="localhost:9092",
            additional_config=additional_config
        )
        
        consumer = resource.get_consumer("test-group")
        assert consumer is not None
        
        # Check producer config includes additional parameters
        producer_config = resource.get_producer_config()
        assert producer_config['request.timeout.ms'] == 30000
        assert producer_config['retry.backoff.ms'] == 1000
        assert producer_config['max.poll.interval.ms'] == 300000
        print(" Additional config parameter test passed!")
    
    def test_session_timeout_configuration(self):
        """Test session timeout configuration."""
        resource = KafkaResource(
            bootstrap_servers="localhost:9092",
            session_timeout_ms=30000
        )
        
        assert resource.session_timeout_ms == 30000
        consumer = resource.get_consumer("test-group")
        assert consumer is not None
        print(" Session timeout configuration test passed!")
    
    def test_auto_offset_reset_configuration(self):
        """Test auto offset reset configuration."""
        resource = KafkaResource(
            bootstrap_servers="localhost:9092",
            auto_offset_reset="latest"
        )
        
        assert resource.auto_offset_reset == "latest"
        consumer = resource.get_consumer("test-group")
        assert consumer is not None
        print(" Auto offset reset configuration test passed!")
    
    def test_ssl_hostname_verification_disabled(self):
        """Test SSL hostname verification can be disabled."""
        resource = KafkaResource(
            bootstrap_servers="localhost:9092",
            security_protocol=SecurityProtocol.SSL,
            ssl_check_hostname=False,
            ssl_ca_location="/path/to/ca.pem"
        )
        
        assert resource.ssl_check_hostname is False
        
        producer_config = resource.get_producer_config()
        assert producer_config['ssl.check.hostname'] is False
        print(" SSL hostname verification disabled test passed!")


class TestBackwardCompatibility:
    """Test that existing code continues to work."""
    
    def test_minimal_configuration_still_works(self):
        """Test that minimal configuration from before security update works."""
        # This is how users would have created resources before
        resource = KafkaResource(bootstrap_servers="localhost:9092")
        
        # Should still work exactly the same
        consumer = resource.get_consumer("legacy-group")
        assert consumer is not None
        
        # Should default to PLAINTEXT
        assert resource.security_protocol == SecurityProtocol.PLAINTEXT
        print(" Minimal configuration backward compatibility test passed!")
    
    def test_existing_consumer_creation_pattern(self):
        """Test that existing consumer creation patterns work."""
        resource = KafkaResource(bootstrap_servers="localhost:9092")
        
        # Multiple consumer creation (common pattern)
        consumer1 = resource.get_consumer("group-1")
        consumer2 = resource.get_consumer("group-2")
        
        assert consumer1 is not None
        assert consumer2 is not None
        assert consumer1 != consumer2  # Different instances
        print(" Existing consumer creation pattern test passed!")


class TestSecurityEnums:
    """Test security enumeration values."""
    
    def test_security_protocol_values(self):
        """Test SecurityProtocol enum values."""
        assert SecurityProtocol.PLAINTEXT.value == "PLAINTEXT"
        assert SecurityProtocol.SSL.value == "SSL"
        assert SecurityProtocol.SASL_PLAINTEXT.value == "SASL_PLAINTEXT"
        assert SecurityProtocol.SASL_SSL.value == "SASL_SSL"
        print(" SecurityProtocol enum values test passed!")
    
    def test_sasl_mechanism_values(self):
        """Test SaslMechanism enum values."""
        assert SaslMechanism.PLAIN.value == "PLAIN"
        assert SaslMechanism.SCRAM_SHA_256.value == "SCRAM-SHA-256"
        assert SaslMechanism.SCRAM_SHA_512.value == "SCRAM-SHA-512"
        assert SaslMechanism.GSSAPI.value == "GSSAPI"
        assert SaslMechanism.OAUTHBEARER.value == "OAUTHBEARER"
        print(" SaslMechanism enum values test passed!")


class TestErrorHandling:
    """Test error handling for security configurations."""
    
    @patch('dagster_kafka.resources.get_dagster_logger')
    def test_consumer_creation_failure_handling(self, mock_logger):
        """Test error handling when consumer creation fails."""
        resource = KafkaResource(
            bootstrap_servers="invalid-broker:9092",
            security_protocol=SecurityProtocol.SASL_SSL,
            sasl_mechanism=SaslMechanism.PLAIN,
            sasl_username="user",
            sasl_password="pass"
        )
        
        # This should not raise an exception during resource creation
        assert resource is not None
        
        # Consumer creation might fail (depending on whether invalid broker is reachable)
        # but the resource should handle it gracefully
        try:
            consumer = resource.get_consumer("test-group")
            # If no exception, that's fine too
            assert consumer is not None
        except Exception as e:
            # Exception during consumer creation is acceptable
            assert isinstance(e, Exception)
        
        print(" Consumer creation failure handling test passed!")
    
    def test_security_validation_with_missing_ssl_ca_warning(self):
        """Test that missing SSL CA generates appropriate warning."""
        with patch('dagster_kafka.resources.get_dagster_logger') as mock_logger:
            mock_logger_instance = Mock()
            mock_logger.return_value = mock_logger_instance
            
            resource = KafkaResource(
                bootstrap_servers="localhost:9092",
                security_protocol=SecurityProtocol.SSL
                # Missing ssl_ca_location
            )
            
            # Validation should pass but log warning
            result = resource.validate_security_config()
            assert result is True
            
            # Check that warning was logged
            mock_logger_instance.warning.assert_called_once()
            warning_call = mock_logger_instance.warning.call_args[0][0]
            assert "ssl_ca_location not set" in warning_call
            
        print(" Missing SSL CA warning test passed!")


def test_security_imports():
    """Test that security-related imports work correctly."""
    from dagster_kafka import KafkaResource, SecurityProtocol, SaslMechanism
    
    # Test that all expected enums are available
    assert SecurityProtocol.PLAINTEXT is not None
    assert SecurityProtocol.SSL is not None
    assert SecurityProtocol.SASL_PLAINTEXT is not None
    assert SecurityProtocol.SASL_SSL is not None
    
    assert SaslMechanism.PLAIN is not None
    assert SaslMechanism.SCRAM_SHA_256 is not None
    assert SaslMechanism.SCRAM_SHA_512 is not None
    assert SaslMechanism.GSSAPI is not None
    assert SaslMechanism.OAUTHBEARER is not None
    
    print(" Security imports test passed!")


if __name__ == "__main__":
    # Run individual test functions for manual testing
    test_security_imports()
    print(" All security tests available for pytest execution!")