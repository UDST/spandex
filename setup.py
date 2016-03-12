import os.path

# Install setuptools if not installed.
try:
    import setuptools
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()

from setuptools import setup, find_packages


# read README as the long description
readme = 'README' if os.path.exists('README') else 'README.md'
with open(readme, 'r') as f:
    long_description = f.read()

setup(
    name='spandex',
    version='0.1dev',
    description='Spatial Analysis and Data Extraction',
    long_description=long_description,
    author='UrbanSim Inc.',
    author_email='udst@urbansim.com',
    url='https://github.com/udst/spandex',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Programming Language :: Python :: 2.7',
    ],
    packages=find_packages(exclude=['*.tests']),
    install_requires=[
        'GeoAlchemy2>=0.2.1',  # Bug fix for schemas other than public.
        'pandas>=0.15.0',      # pandas.Index.difference.
        'psycopg2>=2.5',       # connection and cursor context managers.
        'six>=1.4',            # Mapping for urllib.
        'SQLAlchemy==0.9.9'      # GeoAlchemy2 support.
    ],
    extras_require={
        'gdal': ['GDAL>=1.7'],     # Python 3 support.
        'plot': ['pygraphviz'],
        'sim': ['urbansim>=1.3'],  # TableFrame support and sim.table caching.
    }
)
