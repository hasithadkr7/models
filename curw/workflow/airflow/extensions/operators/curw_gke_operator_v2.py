import asyncio
import datetime as dt
import logging

from kubernetes import client, config

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from curw.workflow.airflow import utils as af_utils


class CurwGkeOperatorV2Exception(Exception):
    pass


K8S_API_VERSION_TAG = 'v1'


class CurwGkeOperatorV2(BaseOperator):
    """

    """
    template_fields = ['kube_config_path', 'pod_name', 'namespace', 'container_names', 'container_commands',
                       'container_args_lists']

    @apply_defaults
    def __init__(
            self,
            pod,
            pod_name=None,
            namespace=None,
            kube_config_path=None,
            secret_list=None,
            api_version=None,
            auto_remove=False,
            poll_interval=dt.timedelta(seconds=60),
            *args,
            **kwargs):

        super(CurwGkeOperatorV2, self).__init__(*args, **kwargs)
        self.api_version = api_version or K8S_API_VERSION_TAG
        self.kube_config_path = kube_config_path
        self.pod = pod

        self.pod_name = pod_name or self.pod.metadata.name
        self.namespace = namespace or self.pod.metadata.namespace or 'default'

        self.container_names = []
        self.container_commands = []
        self.container_args_lists = []
        for c in pod.spec.containers:
            self.container_names.append(c.name)
            self.container_commands.append(c.command)
            self.container_args_lists.append(c.args)

        self.auto_remove = auto_remove
        self.secrets_list = secret_list or []

        self.kube_client = None

        self.poll_interval = poll_interval

    def _wait_for_pod_completion(self):
        async def poll_kube(kube_future, kube_client, name, namespace):
            start_t = dt.datetime.now()
            while True:
                try:
                    pod = kube_client.read_namespaced_pod_status(name=name, namespace=namespace)
                    logging.info(
                        "Pod status: %s elapsed time: %s" % (pod.status.phase, str(dt.datetime.now() - start_t)))
                    status = pod.status.phase
                    if status == 'Succeeded' or status == 'Failed':
                        log = 'Pod log:\n' + kube_client.read_namespaced_pod_log(name=name, namespace=namespace,
                                                                                 timestamps=True, pretty='true')
                        kube_future.set_result('Pod exited! %s %s %s\n%s' % (namespace, name, status, log))
                except Exception as e:
                    logging.error('Error in polling pod %s:%s' % (name, str(e)))
                    kube_future.set_exception(e)
                await asyncio.sleep(self.poll_interval.seconds)

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        future = asyncio.Future()
        asyncio.ensure_future(poll_kube(future, self.kube_client, self.pod_name, self.namespace))
        loop.run_until_complete(future)
        logging.info(future.result())

        return True

        # w = watch.Watch()
        # deletable = True
        # for event in w.stream(self.kube_client.list_namespaced_pod, self.namespace):
        #     logging.info("Event: %s %s %s" % (event['type'], event['object'].kind, event['object'].metadata.name))
        #     logging.debug(event)
        #     if (event['object'].metadata.namespace, event['object'].metadata.name) == (self.namespace, self.pod_name):
        #         if event['object'].status.phase == 'Succeeded':
        #             logging.info('Pod completed successfully! %s %s' % (self.namespace, self.pod_name))
        #             break
        #         elif event['object'].status.phase == 'Failed':
        #             logging.error('Pod failed! %s %s' % (self.namespace, self.pod_name))
        #             break
        #         if event['type'] == 'DELETED':
        #             logging.warning('Pod deleted! %s %s' % (self.namespace, self.pod_name))
        #             deletable = False
        #             break
        # w.stop()
        #
        # if deletable:
        #     logging.info(
        #         'Pod log:\n' + self.kube_client.read_namespaced_pod_log(name=self.pod_name, namespace=self.namespace,
        #                                                                 timestamps=True, pretty='true'))
        # return deletable

    def _create_secrets(self):
        if self.kube_client is not None:
            avail_secrets = [i.metadata.name for i in
                             self.kube_client.list_namespaced_secret(namespace=self.namespace).items]
            for secret in self.secrets_list:
                if secret.metadata.name not in avail_secrets:
                    self.kube_client.create_namespaced_secret(namespace=self.namespace, body=secret)
                else:
                    logging.info('Secret exists ' + secret.metadata.name)

    def execute(self, context):
        logging.info('Updating pod with templated fields')
        self.pod.metadata.name = af_utils.sanitize_name(self.pod_name)
        self.pod.metadata.namespace = af_utils.sanitize_name(self.namespace)
        for i in range(len(self.container_names)):
            self.pod.spec.containers[i].name = af_utils.sanitize_name(self.container_names[i])
            self.pod.spec.containers[i].command = self.container_commands[i]
            self.pod.spec.containers[i].args = self.container_args_lists[i]

        logging.info('Initializing kubernetes config from file ' + str(self.kube_config_path))
        config.load_kube_config(config_file=self.kube_config_path)

        logging.info('Initializing kubernetes client for API version ' + self.api_version)
        if self.api_version.lower() == K8S_API_VERSION_TAG:
            self.kube_client = client.CoreV1Api()
        else:
            raise CurwGkeOperatorV2Exception('Unsupported API version ' + self.api_version)

        logging.info('Creating secrets')
        self._create_secrets()

        logging.info('Creating namespaced pod')
        logging.debug('Pod config ' + str(self.pod))
        self.kube_client.create_namespaced_pod(namespace=self.namespace, body=self.pod)

        logging.info('Waiting for pod completion')
        deletable = self._wait_for_pod_completion()

        if self.auto_remove and deletable:
            self.on_kill()

    def on_kill(self):
        if self.kube_client is not None:
            logging.info('Stopping kubernetes pod')
            self.kube_client.delete_namespaced_pod(name=self.pod_name, namespace=self.namespace,
                                                   body=client.V1DeleteOptions())
