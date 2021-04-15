import os
import pkgutil
from typing import TypedDict, Tuple, NamedTuple, List, Union, Iterable

import yaml
from prefect.run_configs import KubernetesRun, RunConfig
from prefect.schedules import Schedule
from prefect.storage import Storage
from prefect.storage.docker import Docker

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