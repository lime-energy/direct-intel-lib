import os
from typing import Union, Iterable

import yaml
from pkg_resources import resource_stream
from prefect.run_configs import KubernetesRun

DEFAULT_JOB_TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), "job_template.yaml")


def SplitgraphKubernetesRun(
        image: str = None,
        env: dict = None,
        cpu_limit: Union[float, str] = None,
        cpu_request: Union[float, str] = None,
        memory_limit: str = None,
        memory_request: str = None,
        service_account_name: str = None,
        image_pull_secrets: Iterable[str] = None,
        labels: Iterable[str] = None,
) -> None:

    job_template = yaml.safe_load(read_bytes_from_path(DEFAULT_JOB_TEMPLATE_PATH))
    return KubernetesRun(
        job_template=job_template,
        image=image,
        env=env,
        cpu_limit=cpu_limit,
        cpu_request=cpu_request,
        memory_limit=memory_limit,
        memory_request=memory_request,
        service_account_name=service_account_name,
        image_pull_secrets=image_pull_secrets,
        labels=labels,
    )
