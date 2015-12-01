from setuptools import setup, find_packages
import os
import glob
import numpy as np


setup(
    name = 'cos_monitoring',
    version = '0.0.1',
    description = 'Provide utilities and monotiring of cos data',
    author = 'Justin Ely',
    author_email = 'ely@stsci.edu',
    keywords = ['astronomy'],
    classifiers = ['Programming Language :: Python',
                   'Programming Language :: Python :: 3',
                   'Development Status :: 1 - Planning',
                   'Intended Audience :: Science/Research',
                   'Topic :: Scientific/Engineering :: Astronomy',
                   'Topic :: Scientific/Engineering :: Physics',
                   'Topic :: Software Development :: Libraries :: Python Modules'],
    packages = find_packages(),
    requires = ['numpy', 'scipy', 'astropy'],
    entry_points = {'console_scripts': ['clean_slate=cos_monitoring.database:clean_slate',
                                        'create_master_csv=scripts.create_master_csv:main'],
    },
    install_requires = ['setuptools',
                        'numpy',
                        'astropy>=1.0.1',
                        'sqlalchemy',
                        'pymysql']
    )
