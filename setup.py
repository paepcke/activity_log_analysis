#import multiprocessing
from setuptools import setup, find_packages
from setuptools.command.test import test as setup_test
import os
import glob

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name = "activity_log_analysis",
    version = "0.0.1",
    #****packages = find_packages(),
    test_suite='nose2.collector.collector',
    # Dependencies on other packages:
    # Couldn't get numpy install to work without
    # an out-of-band: sudo apt-get install python-dev
    setup_requires   = ['pytest-runner'],
    install_requires = ['pymysql-utils>=2.1.5',
                        'logging-singleton>=1.0',
                        'explorecourses>=1.0.6',
                        ],

    tests_require    =[
                       'testfixtures>=6.14.1',
                       ],

    # metadata for upload to PyPI
    author = "Andreas Paepcke",
    author_email = "paepcke@cs.stanford.edu",
    description = "Carta activity log analysis",
    long_description_content_type = "text/markdown",
    long_description = long_description,
    license = "BSD",
    keywords = "log analysis",
    url = "git@github.com:paepcke/activity_log_analysis.git",   # project home page, if any
)

print("To run tests, type 'nose2'")

