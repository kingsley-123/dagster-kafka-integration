from setuptools import setup, find_packages

setup(
    name="dagster-kafka",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "dagster",
        "confluent-kafka",
    ],
)