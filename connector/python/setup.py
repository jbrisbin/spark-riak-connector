
import os

from setuptools import setup, find_packages
from codecs import open
from os import path

basedir = os.path.dirname(os.path.abspath(__file__))
os.chdir(basedir)

with open(path.join(basedir, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()
	
setup(
	name='pyspark_riak',
	version="1.0.0",
	description='Utilities to asssist in working with Riak KV and PySpark.',
	long_description=long_description,
	license='Apache License 2.0',	
    author='Basho Technologies',
    author_email='dataplatform@basho.com',
	url='https://github.com/basho/spark-riak-connector/',
	options={'easy_install': {'allow_hosts': 'pypi.python.org'}},
	platforms='Platform Independent',
	keywords='riak spark pyspark',
	classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',

        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
		
		'Topic :: Database',
		'Topic :: Software Development :: Libraries',
		'Topic :: Scientific/Engineering :: Information Analysis',
		'Topic :: Utilities',
    ],
	packages=find_packages(),
	include_package_data=True,
)
