
import os

from setuptools import setup, find_packages


basedir = os.path.dirname(os.path.abspath(__file__))
os.chdir(basedir)

setup(
	name='pyspark_riak',
	version="1.0.0",
	description='Utilities to asssist in working with Riak KV and PySpark.',
	license='Apache License 2.0',

	packages=find_packages(),
	include_package_data=True,
)
