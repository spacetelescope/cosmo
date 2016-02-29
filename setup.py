from setuptools import setup, find_packages
import os
import glob
import numpy as np


setup(
      name = 'therm_corr',
      version = '0.0.1',
      description = 'Check linearity of COS FUV Thermal Correction',
      author = 'Mees Fix',
      author_email = 'mfix@stsci.edu',
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
      entry_points = {'console_scripts': ['perform_all=scripts.gridwire_revisit:main'
                                          ],
      },
      install_requires = ['setuptools',
                          'numpy',
                          'astropy>=1.0.1'
                          ]
      )
