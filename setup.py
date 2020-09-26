import os
from setuptools import setup

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "dynamesa",
    version = "0.0.1",
    author = "James Robert",
    author_email = "me@jiaaro.com",
    description = ("A simple dynamodb client"),
    license = "MIT",
    keywords = "dynamodb",
    url = "http://jiaaro.com",
    packages=['dynamesa'],
    long_description=read('_README.md'),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: MIT License",
    ],
)