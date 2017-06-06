from distutils.core import setup

setup(
    name='curw',
    version='1.0.0',
    packages=['curwrf', 'curwrf.wrf', 'curwrf.wrf.execution', 'curwrf.wrf.resources', 'curwrf.wrf.extraction',
              'curwrf.realtime', 'curwrf.workflow', 'curwrf.workflow.airflow', 'curwrf.workflow.airflow.dags'],
    url='',
    license='Apache 2.0',
    author='niranda perera',
    author_email='niranda.17@cse.mrt.ac.lk',
    description=''
)
