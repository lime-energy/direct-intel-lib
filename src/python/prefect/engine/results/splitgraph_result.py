import os
import pandas as pd
import pendulum
import re
from contextlib import contextmanager
from typing import Any, Dict
import uuid
from prefect import task
from prefect.engine.result import Result
from prefect.utilities.collections import DotDict
from pandas_schema import Schema
from splitgraph.config.config import create_config_dict, patch_config
from splitgraph.core.engine import repository_exists, get_engine
from splitgraph.core.repository import Repository, clone, table_exists_at
from splitgraph.engine.postgres.engine import PostgresEngine
from splitgraph.ingestion.pandas import df_to_table, sql_to_df
from splitgraph.splitfile.execution import execute_commands

from .errors import SchemaValidationError

project_name = os.environ.get('PREFECT_PROJECT_NAME')

class SplitgraphResult(Result):
    repo_pattern = re.compile('(?P<namespace>.*)/(?P<repo>.*):(?P<tag>.*)/(?P<table>.*)')

    """
    Result that is written to and retrieved from Splitgraph.
    Splitgraph uses the following env vars

    If getting a `Connection refused` error, check Splitgraph's configuration.
    You can set the the environment vars, or use the `env` param to configure it.

    ```bash
    SG_ENGINE_HOST='<localhost>',
    SG_ENGINE_PORT=<5432>,
    SG_ENGINE_USER='<sgr>',
    SG_ENGINE_PWD='<supersecure>',
    SG_ENGINE_DB_NAME='<splitgraph>',
    ```

    Args:
        - namespace (str, optional): Splitgraph namespace
        - location (str, optional): Fully qualified splitgraph table, i.e. namespace/repository:tag/table.
            Follows the logic of `.format(**kwargs)`.
        - comment (str, optional): Possibly templated table to be used for commenting the
            Splitgraph commit.
        - env (Dict[str, Any], optional): Env vars to be patched to Splitgraph default
            configuration.
        - **kwargs (Any, optional): any additional `Result` initialization options
    """

    def __init__(
        self,
        auto_init_repo: str = False,
        comment: str = None,
        env: Dict[str, Any] = None,
        auto_push: bool = True,
        layer_query: bool = False,
        remote_name: str = 'bedrock',
        schema: Schema = None,
        **kwargs: Any
    ) -> None:
        self.env = env or dict()
        self.auto_init_repo = auto_init_repo
        self.comment = comment
        self.auto_push = auto_push
        self.layer_query = layer_query
        self.remote_name = remote_name
        self.schema = schema
       

        super().__init__(**kwargs)
    
    @property
    def engine(self) -> PostgresEngine:
        if getattr(self, "_engine", None) is None:
            cfg = patch_config(create_config_dict(), self.env or dict())
            engine = PostgresEngine(name='SplitgraphResult', conn_params=cfg)
            engine.initialize()

            self._engine = engine
        return self._engine
    @property
    def repo_info(self) -> DotDict:
        return DotDict(self.repo_pattern.search(self.location).groupdict())
    @property
    def default_location(self) -> str:
        location = f"{project_name}/{flow_name}:{tag or uuid.uuid4()}/prefect_result"
        return location

    @task
    def build_splitfile(self, splitfile_commands: str,  **kwargs: Any):
        formatting_kwargs = {
            **kwargs,
            **prefect.context.get("parameters", {}).copy(),
            **prefect.context,            
        }
        new = self.format(**formatting_kwargs)
        repo = Repository(namespace=new.repo_info.namespace, repository=new.repo_info.repo, engine=self.engine)
        execute_commands(splitfile_commands, formatting_kwargs, repo)
        if self.auto_push:
            repo.push(
                self.get_upstream(repo),
                handler="S3",
                overwrite_objects=True,
                overwrite_tags=True,
                reupload_objects=True,
            )

    @task
    def fetch(self, **kwargs: Any):
        formatting_kwargs = {
            **kwargs,
            **prefect.context.get("parameters", {}).copy(),
            **prefect.context,            
        }
        new = self.format(**formatting_kwargs)
        result = new.read(new.location)
        return result.value
        
    def to_repo_location(self) -> str:
        return "{namespace}/{repo}:{tag}".format(**self.repo_info)

    def read(self, location: str) -> Result:
        new = self.copy()
        new.location = location
        try:            
        
            repo = Repository(namespace=new.repo_info.namespace, repository=new.repo_info.repo, engine=self.engine)

            assert self.engine.connected

            cloned_repo=clone(
                self.get_upstream(repo),
                local_repository=repo,
                download_all=True,
                overwrite_objects=True,
                overwrite_tags=True,
                single_image=new.repo_info.tag,
            )
            data = sql_to_df(f"SELECT * FROM {new.repo_info.table}", repository=cloned_repo, use_lq=self.layer_query)

            if self.schema is not None:
                errors = self.schema.validate(data)
                if errors:
                    raise SchemaValidationError(errors)



            new.value = new.serializer.deserialize(stream.getvalue())
        except Exception as exc:
            self.logger.exception(
                "Unexpected error while reading from result handler: {}".format(
                    repr(exc)
                )
            )
            raise exc
        
        return new

    def write(self, value_: Any, **kwargs: Any) -> Result:
        """
        Writes the result to a repository on Splitgraph


        Args:
            - value_ (Any): the value to write; will then be stored as the `value` attribute
                of the returned `Result` instance
            - **kwargs (optional): if provided, will be used to format the `table`, `comment`, and `tag`

        Returns:
            - Result: returns a new `Result` with both `value`, `comment`, `table`, and `tag` attributes
        """

        if self.schema is not None:
            errors = self.schema.validate(value_)
            if errors:
                raise SchemaValidationError(errors)


        new = self.format(**kwargs)
        new.value = value_

        repo_info = DotDict(repo_pattern.search(new.location))
    
        repo = Repository(namespace=repo_info.namespace, repository=repo_info.repo, engine=self.engine)

        assert isinstance(value_, pd.DataFrame)
        assert self.engine.connected

        if not repository_exists(repo) and self.auto_init_repo:
            self.logger.info("Creating repo {}/{}...".format(repo.namespace, repo.repository))
            repo.init()

        # TODO: Retrieve the repo from bedrock first

        self.logger.info("Starting to upload result to {}...".format(new.location))

        with self.atomic(self.engine):
            self.logger.info("checkout")
            img = repo.head
            img.checkout(force=True)

            self.logger.info("df to table")
            df_to_table(new.value, repository=repo, table=repo_info.table, if_exists='replace')

            self.logger.info("commit")
            new_img = repo.commit(comment=new.comment, chunk_size=10000)
            new_img.tag(repo_info.tag)


        # if (repo.diff(new.table, img, new_img)):
        if self.auto_push:
            self.logger.info("push")
            repo.push(
                self.get_upstream(repo),
                handler="S3",
                overwrite_objects=True,
                overwrite_tags=True,
                reupload_objects=True,
            )

        self.logger.info("Finished uploading result to {}...".format(new.location))

        return new

    def exists(self, location: str, **kwargs: Any) -> bool:
        """
        Checks whether the target result exists in the file system.

        Does not validate whether the result is `valid`, only that it is present.

        Args:
            - location (str): Location of the result in the specific result target.
                Will check whether the provided location exists
            - **kwargs (Any): string format arguments for `location`

        Returns:
            - bool: whether or not the target result exists
        """

        try:
            repo_info = DotDict(repo_pattern.search(location))
            repo = Repository(namespace=repo_info.namespace, repository=repo_info.repo, engine=self.engine)

            assert self.engine.connected
 
            table_exists_at(self.get_upstream(repo), repo_info.table)
            return self.client.get_object(Bucket=self.bucket, Key=location.format(**kwargs))

        except Exception as exc:
            self.logger.exception(
                "Unexpected error while reading from Splitgraph: {}".format(repr(exc))
            )
            raise

    @contextmanager
    def atomic(self, engine):
        try:
            yield
        finally:
            self.logger.info("engine commit")
            engine.commit()

    def get_upstream(self, repository: Repository):
        return Repository.from_template(repository, engine=get_engine(self.remote_name, autocommit=True))
