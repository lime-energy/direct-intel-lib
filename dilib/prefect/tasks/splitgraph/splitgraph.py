from typing import Any, Dict

import prefect
from dilib.splitgraph import SchemaValidationError, RepoInfo, RepoInfoDict
from pandas_schema import Schema
from prefect import Task
from prefect.utilities.collections import DotDict
from prefect.utilities.tasks import defaults_from_attrs

from splitgraph.config.config import create_config_dict, patch_config
from splitgraph.core.engine import get_engine
from splitgraph.core.repository import Repository, clone, table_exists_at
from splitgraph.ingestion.pandas import df_to_table, sql_to_df
from splitgraph.splitfile.execution import execute_commands


class SplitgraphFetch(Task):
    """
    Run a query against splitgraph.

    Args:

    Raises:
        - ValueError: if a `result` keyword is passed

    Examples:

    ```python
    
    ```

    """

    def __init__(
      self, 
      query: str = None,
      repo_dict: RepoInfoDict = None,
      schema: Schema = None,
      layer_query: bool = False,
      **kwargs
    ) -> None:
        self.repo_dict = repo_dict
        self.query = query
        self.schema = schema
        self.layer_query = layer_query
        
        super().__init__(**kwargs)

    @defaults_from_attrs('repo_dict', 'query')
    def run(self, repo_dict: RepoInfoDict = None, query: str = None, **kwargs: Any):
        """  

        Args:

        Returns:
            - No return
        """
        assert repo_dict, 'Must specify repo.'
        repo_info = RepoInfo(**repo_dict)
        repo = Repository(namespace=repo_info.namespace, repository=repo_info.repository)
       
        data = sql_to_df(self.query, repository=repo, use_lq=self.layer_query)

        if self.schema is not None:
            errors = self.schema.validate(data)
            if errors:
                raise SchemaValidationError(errors)
        
        return data
