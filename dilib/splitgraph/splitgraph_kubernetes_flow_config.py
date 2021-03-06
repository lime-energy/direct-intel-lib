import os
import pkgutil
from typing import TypedDict, Tuple, NamedTuple, List, Union, Iterable

import yaml
from prefect.run_configs import KubernetesRun, RunConfig
from prefect.schedules import Schedule
from prefect.storage import Storage
from prefect.storage.docker import Docker

DEFAULT_PREFECT_ENV = 'production'
DEFAULT_REMOTE = 'bedrock'

class DilibContext(NamedTuple):
    prefect_env: str
    default_remote_name: str
    env_namespace_prefix: str
    project_name: str
    default_env: dict
class SplitgraphKubernetesFlowConfig(TypedDict, total=False):
    run_config: RunConfig
    storage: Storage
class SplitgraphKubernetesFlowReg(TypedDict, total=False):
    project_name: str
    build: bool
    labels: List[str] = None
    set_schedule_active: bool = True
    version_group_id: str = None
    no_url: bool = False
    idempotency_key: str = None
class CurrentContext(object):
    def __init__(self):
        prefect_env = os.environ.get('PREFECT_ENV') or DEFAULT_PREFECT_ENV
        default_remote_name = os.environ.get(
            'DEFAULT_REMOTE_NAME') if 'DEFAULT_REMOTE_NAME' in os.environ else DEFAULT_REMOTE
        env_namespace_prefix = os.environ.get('ENV_NAMESPACE_PREFIX') or ''
        project_name = os.environ.get('PREFECT_PROJECT_NAME')

        default_env = {
            'DEFAULT_REMOTE_NAME': default_remote_name,
            'ENV_NAMESPACE_PREFIX': env_namespace_prefix,
            'PREFECT_PROJECT_NAME': project_name,
            'SG_CONFIG_FILE': '/var/splitgraph/.sgconfig',
        }

        self._dilib_context = DilibContext(prefect_env, default_remote_name, env_namespace_prefix, project_name, default_env)

    def build_standard_config(
        self,
        image_name: str, 
        flow_path: str, 
        env: dict = None,
        cpu_limit: Union[float, str] = None,
        cpu_request: Union[float, str] = None,
        memory_limit: str = None,
        memory_request: str = None,
        service_account_name: str = None,
        image_pull_secrets: Iterable[str] = None,
        labels: Iterable[str] = None,
    ) -> Tuple[SplitgraphKubernetesFlowConfig, SplitgraphKubernetesFlowReg]:    

        image_name_base = os.environ.get('IMAGE_NAME_BASE')
        registry = os.environ.get('REGISTRY')
        registry_base = os.environ.get('REGISTRY_BASE')
        image_version = os.environ.get('IMAGE_VERSION')
        image = f'{registry_base}/{image_name_base}/{image_name}'

        storage = Docker(
            image_name=image,
            image_tag=image_version,
            local_image=True,
            stored_as_script=True,
            path=flow_path,
            registry_url=registry,
        )

        job_template = yaml.safe_load(pkgutil.get_data("dilib.splitgraph", "job_template.yaml"))
        run_config = KubernetesRun(
            job_template=job_template,
            image=image,
            env={
                **env, 
                **self.dilib_context.default_env
            },
            cpu_limit=cpu_limit,
            cpu_request=cpu_request,
            memory_limit=memory_limit,
            memory_request=memory_request,
            service_account_name=service_account_name,
            image_pull_secrets=image_pull_secrets,
            labels=labels,
        )

        flow_config = dict(
            run_config=run_config,
            storage=storage,
        )
        reg_config = dict(
            project_name = self.dilib_context.project_name,
            build = False,
            labels = [
                "k8s",
                f"prefect:{self.dilib_context.prefect_env}",
            ]
        )
        return flow_config, reg_config


    @property
    def dilib_context(self):
        return self._dilib_context
