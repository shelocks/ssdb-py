#!/usr/bin/env python

from setuptools import setup
from ssdb import __version__

setup(
    name='ssdb',
    version=__version__,
    description="Python client for ssdb",
    author='shelocks',
    author_email='happyshelocks@gmail.com',
    license='MIT',
    keywords=['ssdb'],
    packages=['ssdb']

)
