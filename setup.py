from setuptools import setup, find_packages

setup(
    name='curw',
    version='1.0.0',
    packages=find_packages(exclude=['tmp', 'scratch_area']),
    url='',
    license='Apache 2.0',
    author='niranda perera',
    author_email='niranda.17@cse.mrt.ac.lk',
    description='',
    requires=['airflow', 'shapely', 'joblib', 'netCDF4', 'matplotlib', 'imageio'],
    install_requires=['shapely', 'joblib', 'netCDF4', 'matplotlib', 'imageio', 'numpy', 'pandas']
)
