import os
import yaml

from prefect.utilities.filesystems import read_bytes_from_path
from prefect.run_configs import KubernetesRun


DEFAULT_JOB_TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), "job_template.yaml")


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
        *args,
        **kwargs
    ) -> None:
        if kwargs.get("job_template"):
            raise ValueError("job_template not allowed")
        super().__init__(job_template=self.default_template, **kwargs)

    @property
    def default_template(self) -> str:
        return yaml.safe_load(read_bytes_from_path(DEFAULT_JOB_TEMPLATE_PATH))