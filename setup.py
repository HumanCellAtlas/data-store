#!/usr/bin/env python

import glob
from setuptools import setup, find_packages

setup(
    name="fusillade",
    version="0.0.1",
    url="https://github.com/humancellatlas/fusillade",
    license="MIT License",
    author="Andrey Kislyuk",
    author_email="kislyuk@gmail.com",
    description="Federated User Identity Login & Access Decision Engine",
    long_description=open("README.rst").read(),
    install_requires=[
        "boto3 >= 1.5.12, < 2",
        "chalice >= 1.1.0, < 2"
    ],
    extras_require={
        ':python_version == "2.7"': ["enum34 >= 1.1.6, < 2"]
    },
    packages=find_packages(exclude=["test"]),
    scripts=glob.glob("scripts/*"),
    platforms=["MacOS X", "Posix"],
    package_data={"fusillade": ["*.json"]},
    include_package_data=True,
    test_suite="test",
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.6",
        "Topic :: Software Development :: Libraries :: Python Modules"
    ]
)
