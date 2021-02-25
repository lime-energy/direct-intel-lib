from typing import Any, Dict

import prefect
from prefect import Task
from prefect.utilities.collections import DotDict
from prefect.utilities.tasks import defaults_from_attrs
from src.python.splitgraph import SchemaValidationError, parse_repo

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
      uri: str,
      splitfile_commands: str,
      remote_name: str = 'bedrock',
      env: Dict[str, Any] = None,
      auto_push: bool = True,
      **kwargs
    ) -> None:
        self.uri = uri
        self.splitfile_commands = splitfile_commands
        self.remote_name = remote_name
        self.env = env
        self.auto_push = auto_push
        
        super().__init__(**kwargs)

    @defaults_from_attrs('uri', 'splitfile_commands')
    def run(self, uri: str = None, splitfile_commands: str = None, **kwargs: Any):
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

        repo_info = DotDict(parse_repo(uri.format(**formatting_kwargs)))

        repo = Repository(namespace=repo_info.namespace, repository=repo_info.repo)
        remote = Repository.from_template(repo, engine=get_engine(self.remote_name, autocommit=True))
        execute_commands(splitfile_commands, formatting_kwargs, repo)
        repo.head.tag(repo_info.tag)
        if self.auto_push:
            repo.push(
                remote,
                handler="S3",
                overwrite_objects=True,
                overwrite_tags=True,
                reupload_objects=True,
            )
