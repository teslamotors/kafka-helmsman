"""Packaging."""
from setuptools import setup, find_packages

with open("README.md") as f:
    readme = f.read()

setup(
    name="kafka_roller",
    description="Kafka rolling restart",
    long_description=readme,
    use_scm_version={"root": "../", "relative_to": __file__},
    setup_requires=["setuptools_scm"],
    install_requires=[
        "fabric==2.4.0",
        "confluent-kafka==0.11.5",
        "kazoo==2.5.0",
        "requests==2.32.4",
    ],
    entry_points={
        "console_scripts": [
            "kafka_roller=kafka_roller.cli:main",
            "consumer_health=kafka_roller.consumer_health:main",
        ]
    },
    include_package_data=True,
    packages=find_packages(exclude=("tests",)),
)
