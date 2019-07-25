from setuptools import setup, find_packages

setup(
    name='cosmo',
    version='0.0.1',
    description='Monitors for HST/COS',
    keywords=['astronomy'],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: BSD-3 :: Association of Universities for Research in '
        'Astronomy',
        'Operating System :: Linux'
        ],
    python_requires='~=3.7',  # 3.7 and higher, but not 4
    packages=find_packages(),
    install_requires=[
        'setuptools',
        'numpy>=1.11.1',
        'astropy>=1.0.1',
        'plotly',
        'scipy',
        'pyfastcopy',
        'dask',
        'pandas',
        'pytest',
        'pyyaml',
        'peewee',
        'monitorframe',
        'tqdm'
        # 'monitorframe @ git+https://github.com/spacetelescope/monitor-'
        # 'framework#egg=monitorframe'
        ],
    dependency_links=[
        'http://github.com/spacetelescope/monitor-framework/tarball/master#egg'
        '=monitorframe'
        ],
    entry_points={
        'console_scripts':
            ['run_retrieval=cosmo.retrieval.run_cosmo_retrieval:retrieve']}
    )
