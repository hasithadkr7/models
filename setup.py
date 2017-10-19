from setuptools import setup, find_packages

setup(
    name='curw',
    version='2.0.0-snapshot',
    packages=find_packages(exclude=['scratch_area']),
    url='http://www.curwsl.org',
    license='Apache 2.0',
    author='niranda perera',
    author_email='niranda.17@cse.mrt.ac.lk',
    description='Models being developed at the Center for URban Water, Sri Lanka',
    requires=['airflow', 'shapely', 'joblib', 'netCDF4', 'matplotlib', 'imageio', 'scipy', 'geopandas'],
    install_requires=['shapely', 'joblib', 'netCDF4', 'matplotlib', 'imageio', 'numpy', 'pandas', 'scipy']
)
