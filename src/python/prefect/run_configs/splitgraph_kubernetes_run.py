import yaml
from typing import Union, Iterable

from prefect.utilities.filesystems import parse_path
from prefect.run_configs.base import RunConfig
from prefect.run_configs import KubernetesRun

DEFAULT_JOB_TEMPLATE = '''
apiVersion: batch/v1
kind: Job
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: flow
          command: ["/bin/sh", "-c"]
          env:
          - name: SG_ENGINE_HOST
            value: 127.0.0.1
        - name: sgr
          image: splitgraph/engine
          imagePullPolicy: Always
          lifecycle:
            type: Sidecar
          ports:
          - name: postgres
            containerPort: 5432
            protocol: TCP
          env:
          - name: POSTGRES_PASSWORD
            value: supersecure
'''

class SplitgraphKubernetesRun(KubernetesRun):
    """Configure a flow-run to run as a Kubernetes Job.

    By default, configures a job with a flow container and a sidecar running
    a splitgraph engine. The flow default sgr host is configured to the
    splitgraph sidecar.

    Examples:

    Use the defaults set on the agent:

    ```python
    flow.run_config = SplitgraphKubernetesRun()
    ```
    """

    def __init__(
        self,
        *,
        **kwargs
    ) -> None:
        super().__init__(job_template=DEFAULT_JOB_TEMPLATE, **kwargs)
