from setuptools import setup, find_packages

setup(
    name="source-realtor",
    description="Airbyte source connector for Realtor.com API",
    packages=find_packages(),
    install_requires=[
        "airbyte-cdk",
        "requests"
    ],
    package_data={"": ["*.json"]},
)
