from setuptools import setup, find_packages

with open('README.md', encoding='utf-8') as f:
    long_description = f.read()

setup(
    name="dagster-kafka",
    version="1.0.0",
    author="Kingsley Okonkwo",
    description="Complete Kafka integration for Dagster with enterprise DLQ tooling",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/kingsley-123/dagster-kafka-integration",
    packages=find_packages(exclude=["tests"]),
    include_package_data=True,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.9",
    install_requires=[
        "dagster>=1.5.0",
        "kafka-python>=2.0.2",
        "fastavro>=1.8.0",
        "confluent-kafka[avro]>=2.1.0",
        "requests>=2.28.0",
        "protobuf>=4.21.0,<6.0",
        "grpcio-tools>=1.50.0",
        "googleapis-common-protos>=1.56.0",
    ],
    entry_points={
        "console_scripts": [
            "dlq-inspector=dagster_kafka.dlq_tools.dlq_inspector:main",
            "dlq-replayer=dagster_kafka.dlq_tools.dlq_replayer:main",
            "dlq-monitor=dagster_kafka.dlq_tools.dlq_monitor:main",
            "dlq-alerts=dagster_kafka.dlq_tools.dlq_alerts:main",
            "dlq-dashboard=dagster_kafka.dlq_tools.dlq_dashboard:main",
        ],
    },
)