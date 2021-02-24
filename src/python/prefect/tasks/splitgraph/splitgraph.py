from typing import Any, Dict
from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs
from prefect.utilities.collections import DotDict
from splitgraph.config.config import create_config_dict, patch_config
from splitgraph.core.engine import get_engine
from splitgraph.core.repository import Repository, clone, table_exists_at
from splitgraph.engine.postgres.engine import PostgresEngine
from splitgraph.splitfile.execution import execute_commands
from splitgraph.ingestion.pandas import df_to_table, sql_to_df
from pandas_schema import Schema

from src.python.splitgraph.repo_info import parse_repo
from src.python.splitgraph.errors import SchemaValidationError


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
      uri: str,
      query: str,
      schema: Schema = None,
      layer_query: bool = False,
      remote_name: str = 'bedrock',
      env: Dict[str, Any] = None,
      **kwargs
    ) -> None:
        self.uri = uri
        self.query = query
        self.schema = schema
        self.layer_query = layer_query
        self.remote_name = remote_name
        self.env = env
        
        super().__init__(**kwargs)
    @property
    def engine(self) -> PostgresEngine:
        if getattr(self, "_engine", None) is None:
            cfg = patch_config(create_config_dict(), self.env or dict())
            engine = PostgresEngine(name='SplitgraphResult', conn_params=cfg)
            engine.initialize()

            self._engine = engine
        return self._engine
    @defaults_from_attrs('uri', 'query')
    def run(self, uri: str = None, query: str = None, **kwargs: Any):
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

        assert self.engine.connected

        repo = Repository(namespace=repo_info.namespace, repository=repo_info.repository, engine=self.engine)
        remote = Repository.from_template(repo, engine=get_engine(self.remote_name, autocommit=True))
        cloned_repo=clone(
            remote,
            local_repository=repo,
            download_all=True,
            overwrite_objects=True,
            overwrite_tags=True,
            single_image=repo_info.tag,
        )
        data = sql_to_df(self.query, repository=cloned_repo, use_lq=self.layer_query)

        if self.schema is not None:
            errors = self.schema.validate(data)
            if errors:
                raise SchemaValidationError(errors)
        
        return data