from setuptools import setup, find_packages

setup(
    name='cosmo',
    version='0.0.1',
    description='Provide utilities and monotiring of cos data',
    keywords=['astronomy'],
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Development Status :: 1 - Planning',
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering :: Astronomy',
        'Topic :: Scientific/Engineering :: Physics',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ],
    packages=find_packages(),
    requires=['numpy', 'scipy', 'astropy', 'matplotlib'],
    install_requires=[
        'setuptools',
        'numpy>=1.11.1',
        'astropy>=1.0.1',
        'plotly',
        'scipy',
        'pyfastcopy',
        'monitorframe',
        'dask',
        'pandas',
        'pytest',
        'pyyaml',
        'peewee'
    ]
)
