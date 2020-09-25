import os
from setuptools import setup

setup(
    name = "dynamesa",
    version = "0.0.1",
    author = "James Robert",
    author_email = "me@jiaaro.com",
    description = ("A simple dynamodb client"),
    license = "MIT",
    keywords = "dynamodb",
    url = "http://jiaaro.com",
    py_modules=['dynamesa'],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: MIT License",
    ],
)