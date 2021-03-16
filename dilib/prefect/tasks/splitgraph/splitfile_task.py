from typing import Any, Dict

import prefect
from dilib.splitgraph import SchemaValidationError, RepoInfo, RepoInfoDict
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
        output_base: str = None,
        repo_dict: RepoInfoDict = None,
        **kwargs
    ) -> None:
        self.repo_dict = repo_dict
        self.splitfile_commands = splitfile_commands
        self.output_base = output_base


        super().__init__(**kwargs)

    @defaults_from_attrs('repo_dict', 'splitfile_commands', 'output_base')
    def run(self, repo_dict: RepoInfoDict = None, splitfile_commands: str = None, output_base: str = None, **kwargs: Any):
        """

        Args:

        Returns:
            - No return
        """
        formatting_kwargs = {
            **kwargs,
            **prefect.context.get("parameters", {}).copy(),
            **prefect.context,
        }

        assert repo_dict, 'Must specify repo.'
        repo_info = RepoInfo(**repo_dict)

        repo = Repository(namespace=repo_info.namespace, repository=repo_info.repository)
        execute_commands(splitfile_commands, params=formatting_kwargs, output=repo, output_base=output_base)
 
