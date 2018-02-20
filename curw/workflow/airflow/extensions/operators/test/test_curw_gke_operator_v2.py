import base64
import logging
from datetime import timedelta

import os
from kubernetes import client

import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from curw.workflow.airflow.extensions.operators.curw_gke_operator_v2 import CurwGkeOperatorV2

# these args will get passed on to each operator
# you can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'adhoc':False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'trigger_rule': u'all_success'
}


def get_resource_name(resource, context):
    return '--'.join([resource.lower(), str(context['ti'].dag_id).lower(), str(context['ti'].task_id).lower(),
                      str(context['ti'].job_id).lower()])


def read_file(path):
    with open(path, 'r') as f:
        return f.read()


image = 'us.gcr.io/uwcc-160712/curw-wrf-391'
kube_config_path = None
api_version = None
command = ['/bin/bash']
command_args = ['-c',
                'echo abcd; '
                'echo 1234; '
                'ls -r /wrf/geog/wrf_391_geog  | head -5; '
                'cat /wrf/gcs.json ;'
                'echo 4567; '
                'sleep 60s']
cpus = 1.0
environment = None
force_pull = False
mem_limit = None
network_mode = None
tmp_dir = '/tmp/airflow'
user = None
volumes = None
xcom_push = False
xcom_all = False
auto_remove = False
priviliedged = False

logging.info('Creating container spec for image ' + image)
container = client.V1Container(name='test-container',
                               image_pull_policy='IfNotPresent')
container.image = image
container.command = command
container.args = command_args
container.volume_mounts = [client.V1VolumeMount(mount_path='/wrf/geog', name='test-vol'),
                           client.V1VolumeMount(mount_path='/wrf/', name='test-sec-vol')]

logging.info('Initializing pod')
pod = client.V1Pod()

logging.info('Creating pod metadata ')
pod_metadata = client.V1ObjectMeta(name='test-pod')
pod.metadata = pod_metadata

logging.info('Creating volume list')
vols = [client.V1Volume(name='test-vol',
                        gce_persistent_disk=client.V1GCEPersistentDiskVolumeSource(pd_name='wrf-geog-disk1',
                                                                                   read_only=True)),
        client.V1Volume(name='test-sec-vol',
                        secret=client.V1SecretVolumeSource(secret_name='google-app-creds'))
        ]

logging.info('Creating pod spec')
pod_spec = client.V1PodSpec(containers=[container],
                            restart_policy='OnFailure',
                            volumes=vols)
pod.spec = pod_spec

google_app_creds = client.V1Secret(kind='Secret', type='Opaque', metadata=client.V1ObjectMeta(name='google-app-creds'),
                                   data={'gcs.json': base64.b64encode(
                                       read_file(os.getenv('GOOGLE_APPLICATION_CREDENTIALS')).encode()).decode()})
secrets = [google_app_creds]

dag = DAG(
    'gke-test2',
    default_args=default_args,
    schedule_interval=timedelta(days=1))

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = CurwGkeOperatorV2(
    pod=pod,
    secret_list=secrets or [],
    pod_name='test-pod-1',
    auto_remove=True,
    task_id='wrf',
    dag=dag,
)

t1.doc_md = """\
#### Task Documentation
You can document your task using the attributes `doc_md` (markdown),
`doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
rendered in the UI's Task Instance Details page.
![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
"""

dag.doc_md = __doc__

t2 = BashOperator(
    task_id='sleep',
    depends_on_past=False,
    bash_command='sleep 5',
    dag=dag)

templated_command = """
{% for i in range(5) %}
    echo "{{ ds }}"
    echo "{{ macros.ds_add(ds, 7)}}"
    echo "{{ params.my_param }}"
{% endfor %}
"""

t3 = BashOperator(
    task_id='templated',
    depends_on_past=False,
    bash_command=templated_command,
    params={'my_param': 'Parameter I passed in'},
    dag=dag)

t2.set_upstream(t1)
t3.set_upstream(t1)
