from typing import Any, Dict

import prefect
from dilib.splitgraph import (RepoInfo, SchemaValidationError,
                              Workspace, parse_repo)
from prefect import Task
from prefect.utilities.collections import DotDict
from prefect.utilities.tasks import defaults_from_attrs

from splitgraph.config.config import create_config_dict, patch_config
from splitgraph.core.engine import get_engine
from splitgraph.core.repository import Repository
from splitgraph.splitfile.execution import execute_commands


class SplitfileTask(Task):
    """
    Build a splitfile in splitgraph.

    Args:

    Raises:
        - ValueError: if a `result` keyword is passed

    Examples:

    ```python

    ```

    """

    def __init__(
        self,
        splitfile_commands: str = None,
        output: Workspace = None,
        upstream_repos: Dict[str, str] = None,
        **kwargs
    ) -> None:
        self.upstream_repos = upstream_repos
        self.splitfile_commands = splitfile_commands
        self.output = output


        super().__init__(**kwargs)

    @defaults_from_attrs('upstream_repos', 'splitfile_commands', 'output',)
    def run(self, upstream_repos: Dict[str, str] = None, splitfile_commands: str = None, output: Workspace = None, **kwargs: Any):
        """

        Args:

        Returns:
            - No return
        """
        repo_infos = dict((name, parse_repo(uri)) for (name, uri) in upstream_repos.items())
        v1_sgr_repo_uris = dict((name, repo_info.v1_sgr_uri()) for (name, repo_info) in repo_infos.items())
 

        formatting_kwargs = {
            **v1_sgr_repo_uris,
            **kwargs,
            **prefect.context.get("parameters", {}).copy(),
            **prefect.context,
        }


        repo_info = parse_repo(output['repo_uri'])
        repo = Repository(namespace=repo_info.namespace, repository=repo_info.repository)
     
        execute_commands(
            splitfile_commands, 
            params=formatting_kwargs, 
            output=repo, 
            # output_base=output['image_hash'],
        )
 
