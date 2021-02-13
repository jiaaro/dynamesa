from setuptools import setup

setup(
    name="dynamesa",
    version="0.0.2",
    py_modules=["dynamesa"],
    test_modules=["tests"],
    test_suite="tests",
    author="James Robert",
    author_email="me@jiaaro.com",
    description=("A simple dynamodb client"),
    license="MIT",
    keywords="dynamodb",
    url="http://jiaaro.com",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: MIT License",
    ],
)
