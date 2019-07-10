from setuptools import setup, find_packages
import sys, os

version = '0.0'

setup(name='dagcollective',
      version=version,
      description="Collection of common DAGs for Airflow",
      long_description="""\
""",
      classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='airflow dag dataengineering',
      author='Izhar Firdaus',
      author_email='kagesenshi.87@gmail.com',
      url='http://koslab.org',
      license='MIT',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          # -*- Extra requirements: -*-
      ],
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
