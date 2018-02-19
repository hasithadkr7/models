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
            kube_config_path=None,
            api_version=None,
            auto_remove=False,
            *args,
            **kwargs):

        super(CurwGkeOperatorV2, self).__init__(*args, **kwargs)
        self.api_version = api_version or K8S_API_VERSION_TAG
        self.kube_config_path = kube_config_path
        self.pod = pod
        self.pod_name = pod_name
        self.auto_remove = auto_remove

        self.kube_client = None

    def _wait_for_pod_completion(self, namespace, name):
        w = watch.Watch()
        for event in w.stream(self.kube_client.list_namespaced_pod, namespace):
            logging.info(event)
            if (event['object'].metadata.namespace, event['object'].metadata.name) == (namespace, name):
                if event['object'].status.phase == 'Succeeded':
                    logging.info('Pod completed successfully! %s %s' % (namespace, name))
                    w.stop()
                    break
                elif event['object'].status.phase == 'Failed':
                    logging.error('Pod failed! %s %s' % (namespace, name))
                    w.stop()
                    break
                if event['type'] == 'DELETED':
                    logging.warning('Pod deleted! %s %s' % (namespace, name))
                    w.stop()
                    break

    def execute(self, context):
        logging.info('Initializing kubernetes config from file ' + str(self.kube_config_path))
        config.load_kube_config(config_file=self.kube_config_path)

        logging.info('Initializing kubernetes client for API version ' + self.api_version)
        if self.api_version.lower() == K8S_API_VERSION_TAG:
            self.kube_client = client.CoreV1Api()
        else:
            raise CurwGkeOperatorV2Exception('Unsupported API version ' + self.api_version)

        if self.pod_name:
            logging.info('Pod name %s was explicitly provided!' % self.pod_name)
            self.pod.metadata.name = self.pod_name

        logging.info('Creating namespaced pod')
        logging.debug('Pod config ' + str(self.pod))

        ns = self.pod.metadata.namespace if self.pod.metadata.namespace else 'default'
        self.kube_client.create_namespaced_pod(namespace=ns, body=self.pod)

        logging.info('Waiting for pod completion')
        self._wait_for_pod_completion(ns, self.pod.metadata.name)

        logging.info('Pod log:\n' + self.kube_client.read_namespaced_pod_log(name=self.pod.metadata.name,
                                                                             namespace=ns, timestamps=True,
                                                                             pretty='true'))
        if self.auto_remove:
            self.on_kill()

    def on_kill(self):
        if self.kube_client is not None:
            logging.info('Stopping kubernetes pod')
            self.kube_client.delete_namespaced_pod(name=self.pod.metadata.name, namespace=self.pod.metadata.namespace,
                                                   body=client.V1DeleteOptions())
