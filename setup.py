import os.path

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
    description='Spatial Analysis and Data Exploration',
    long_description=long_description,
    author='Synthicity',
    author_email='ejanowicz@synthicity.com',
    license='BSD',
    url='https://github.com/synthicity/spandex',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Programming Language :: Python :: 2.7',
        'License :: OSI Approved :: BSD License'
    ],
    packages=find_packages(exclude=['*.tests']),
    install_requires=[
        'gdal>=1.8.0',
        'GeoAlchemy2>=0.2.1',
        'pandas>=0.13.1',
        'psycopg2>=2.5',
        'SQLAlchemy>=0.8'
    ],
    extras_require={
        'rastertoolz': ['numpy>=1.8.0', 'rasterio>=0.12', 'rasterstats>=0.4',
                        'shapely>=1.3.2']
    }
)
