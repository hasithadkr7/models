import logging

from kubernetes import client, config, watch

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CurwGkeOperatorV2Exception(Exception):
    pass


K8S_API_VERSION_TAG = 'v1'


class CurwGkeOperatorV2(BaseOperator):
    """

    """
    template_fields = ['kube_config_path', 'pod_name']

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
            *args,
            **kwargs):

        super(CurwGkeOperatorV2, self).__init__(*args, **kwargs)
        self.api_version = api_version or K8S_API_VERSION_TAG
        self.kube_config_path = kube_config_path
        self.pod = pod

        self.pod.metadata.name = self.pod_name = pod_name or self.pod.metadata.name
        self.pod.metadata.namespace = self.namespace = namespace or self.pod.metadata.namespace or 'default'

        self.auto_remove = auto_remove
        self.secrets_list = secret_list or []

        self.kube_client = None

    def _wait_for_pod_completion(self):
        w = watch.Watch()
        for event in w.stream(self.kube_client.list_namespaced_pod, self.namespace):
            logging.debug(event)
            if (event['object'].metadata.namespace, event['object'].metadata.name) == (self.namespace, self.pod_name):
                if event['object'].status.phase == 'Succeeded':
                    logging.info('Pod completed successfully! %s %s' % (self.namespace, self.pod_name))
                    w.stop()
                    break
                elif event['object'].status.phase == 'Failed':
                    logging.error('Pod failed! %s %s' % (self.namespace, self.pod_name))
                    w.stop()
                    break
                if event['type'] == 'DELETED':
                    logging.warning('Pod deleted! %s %s' % (self.namespace, self.pod_name))
                    w.stop()
                    break

    def _create_secrets(self):
        if self.kube_client is not None:
            for secret in self.secrets_list:
                self.kube_client.create_namespaced_secret(namespace=self.namespace, body=secret)

    def execute(self, context):
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
        self._wait_for_pod_completion()

        logging.info(
            'Pod log:\n' + self.kube_client.read_namespaced_pod_log(name=self.pod_name, namespace=self.namespace,
                                                                    timestamps=True, pretty='true'))
        if self.auto_remove:
            self.on_kill()

    def on_kill(self):
        if self.kube_client is not None:
            logging.info('Stopping kubernetes pod')
            self.kube_client.delete_namespaced_pod(name=self.pod_name, namespace=self.namespace,
                                                   body=client.V1DeleteOptions())
