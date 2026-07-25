from setuptools import setup, find_packages

setup(
    name="queuectl",
    version="1.0.0",
    packages=find_packages(),
    entry_points={
        "console_scripts": [
            "queuectl=queuectl_engine.cli:main",
        ],
    },
)
