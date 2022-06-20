#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-pleo",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Scalefree International GmbH",
    url="https://scalefree.com",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_pleo"],
    install_requires=[
        # NB: Pin these to a more specific version for tap reliability
        "singer-python",
        "requests",
    ],
    entry_points="""
    [console_scripts]
    tap-pleo=tap_pleo:main
    """,
    packages=["tap_pleo"],
    package_data = {
        "schemas": ["tap_pleo/schemas/*.json"]
    },
    include_package_data=True,
)