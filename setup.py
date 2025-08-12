from setuptools import setup, find_packages
import pathlib

# Read the contents of README file
HERE = pathlib.Path(__file__).parent
README = (HERE / "README.md").read_text(encoding='utf-8')

setup(
    name="dagster-kafka",
    version="1.2.1",
    
    # Author and maintainer information
    author="Kingsley Okonkwo",
    author_email="kingskonk@gmail.com",
    maintainer="Kingsley Okonkwo",
    maintainer_email="kingskonk@gmail.com",
    
    # Package description
    description="Enterprise-grade Kafka integration for Dagster with comprehensive serialization support, DLQ handling, and production monitoring",
    long_description=README,
    long_description_content_type="text/markdown",
    
    # URLs
    url="https://github.com/kingsley-123/dagster-kafka-integration",
    project_urls={
        "Homepage": "https://github.com/kingsley-123/dagster-kafka-integration",
        "Documentation": "https://github.com/kingsley-123/dagster-kafka-integration/blob/main/README.md",
        "Repository": "https://github.com/kingsley-123/dagster-kafka-integration",
        "Bug Reports": "https://github.com/kingsley-123/dagster-kafka-integration/issues",
        "Source": "https://github.com/kingsley-123/dagster-kafka-integration",
        "Changelog": "https://github.com/kingsley-123/dagster-kafka-integration/blob/main/CHANGELOG.md",
        "Validation Report": "https://github.com/kingsley-123/dagster-kafka-integration/tree/main/validation",
        "PyPI": "https://pypi.org/project/dagster-kafka/",
    },
    
    # Package configuration
    packages=find_packages(exclude=["tests*", "examples*", "docs*"]),
    include_package_data=True,
    zip_safe=False,
    
    # Python version requirement
    python_requires=">=3.9",
    
    # Dependencies
    install_requires=[
        "dagster>=1.5.0",
        "kafka-python>=2.0.2",
        "fastavro>=1.8.0",
        "confluent-kafka[avro]>=2.1.0",
        "requests>=2.28.0",
        "protobuf>=4.21.0,<6.0",
        "grpcio-tools>=1.50.0",
        "googleapis-common-protos>=1.56.0",
        "jsonschema>=4.0.0",
    ],
    
    # Optional dependencies
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=22.0.0",
            "isort>=5.10.0",
            "flake8>=5.0.0",
            "mypy>=1.0.0",
        ],
        "docs": [
            "sphinx>=5.0.0",
            "sphinx-rtd-theme>=1.0.0",
            "myst-parser>=0.18.0",
        ],
        "monitoring": [
            "prometheus-client>=0.14.0",
            "slack-sdk>=3.19.0",
        ]
    },
    
    # Console scripts
    entry_points={
        "console_scripts": [
            "dlq-inspector=dagster_kafka.dlq_tools.dlq_inspector:main",
            "dlq-replayer=dagster_kafka.dlq_tools.dlq_replayer:main",
            "dlq-monitor=dagster_kafka.dlq_tools.dlq_monitor:main",
            "dlq-alerts=dagster_kafka.dlq_tools.dlq_alerts:main",
            "dlq-dashboard=dagster_kafka.dlq_tools.dlq_dashboard:main",
        ],
    },
    
    # PyPI classifiers
    classifiers=[
        # Development status
        "Development Status :: 5 - Production/Stable",
        
        # Intended audience
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Intended Audience :: System Administrators",
        
        # Topic
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Distributed Computing",
        "Topic :: System :: Monitoring",
        "Topic :: Database",
        "Topic :: Internet",
        
        # License
        "License :: OSI Approved :: Apache Software License",
        
        # Programming language
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3 :: Only",
        
        # Operating systems
        "Operating System :: OS Independent",
        "Operating System :: POSIX",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: MacOS",
        
        # Environment
        "Environment :: Console",
        "Environment :: No Input/Output (Daemon)",
        
        # Natural language
        "Natural Language :: English",
    ],
    
    # Keywords for discoverability
    keywords=[
        # Core technologies
        "dagster", "kafka", "apache-kafka", "streaming", 
        
        # Data engineering
        "data-engineering", "data-pipeline", "etl", "data-processing",
        
        # Serialization formats
        "json", "json-schema", "avro", "protobuf", "serialization",
        
        # Enterprise features
        "enterprise", "production", "monitoring", "alerting", "dlq", 
        "dead-letter-queue", "error-handling", "circuit-breaker",
        
        # Infrastructure
        "microservices", "distributed-systems", "real-time", 
        "schema-registry", "confluent", "data-validation",
        
        # Security
        "sasl", "ssl", "security", "authentication", "authorization",
    ],
    
    # Package metadata
    platforms=["any"],
    
    # License
    license="Apache-2.0",
    license_files=["LICENSE"],
    
    # Additional metadata
    provides=["dagster_kafka"],
)