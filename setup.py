#!/usr/bin/env python
from setuptools import find_packages, setup
setup(
    name="mysql-cli",
    packages=find_packages(
        include=[
            "mysql_cli",
        ]
    )
)
