from setuptools import setup, find_packages, Extension
import os
import glob
import numpy as np

cci_read_module = Extension('cos_monitoring.cci.cci_read',
                            sources = ['cos_monitoring/cci/cci_read.c'],
                            libraries=['cfitsio'],
                            include_dirs=[np.get_include()]
                             )

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
    requires = ['numpy', 'scipy', 'astropy', 'matplotlib'],
    entry_points = {'console_scripts': ['clean_slate=cos_monitoring.database:clean_slate',
                                        'do_all=cos_monitoring.database:do_all',
                                        'run_all_monitors=cos_monitoring.database:run_all_monitors',
                                        'create_master_csv=scripts.create_master_csv:main',
					                    'create_reports=cos_monitoring.database.report:query_all'],
    },
    install_requires = ['setuptools',
                        'numpy',
                        'astropy>=1.0.1',
                        'sqlalchemy',
                        'pymysql',
                        'matplotlib'],
    ext_modules = [cci_read_module ]
    )
