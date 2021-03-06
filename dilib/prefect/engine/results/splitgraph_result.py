import os
import uuid
from contextlib import contextmanager
from typing import Any, Dict

import pandas as pd
from dilib.splitgraph import SchemaValidationError, parse_repo, RepoInfo
from pandas_schema import Schema
from prefect import Task, task
from prefect.engine.result import Result
from prefect.utilities.collections import DotDict
from splitgraph.config.config import create_config_dict, patch_config
from splitgraph.core.engine import get_engine, repository_exists
from splitgraph.core.repository import Repository, clone, table_exists_at
from splitgraph.engine.postgres.engine import PostgresEngine
from splitgraph.ingestion.pandas import df_to_table, sql_to_df


class SplitgraphResult(Result):
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
        schema: Schema = None,
        **kwargs: Any
    ) -> None:
        self.env = env or dict()
        self.auto_init_repo = auto_init_repo
        self.comment = comment
        self.auto_push = auto_push
        self.layer_query = layer_query
        self.schema = schema
        
        super().__init__(**kwargs)
    
   
    @property
    def repo_info(self) -> RepoInfo:
        return parse_repo(self.location)
    @property
    def default_location(self) -> str:
        location = "{flow_name}/{task_name}:{tag or uuid.uuid4()}/prefect_result"
        return location

    def read(self, location: str) -> Result:
        new = self.copy()
        new.location = location
        try:            
        
            repo = Repository(namespace=new.repo_info.namespace, repository=new.repo_info.repository)
            remote = Repository.from_template(repo, engine=get_engine(new.repo_info.remote_name, autocommit=True))

            cloned_repo=clone(
                remote,
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



            new.value = data
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

        repo_info = parse_repo(new.location)
    
        repo = Repository(namespace=repo_info.namespace, repository=repo_info.repository)
        remote = Repository.from_template(repo, engine=get_engine(repo_info.remote_name, autocommit=True))
       
        assert isinstance(value_, pd.DataFrame)
       

        if not repository_exists(repo) and self.auto_init_repo:
            self.logger.info("Creating repo {}/{}...".format(repo.namespace, repo.repository))
            repo.init()

        # TODO: Retrieve the repo from bedrock first

        self.logger.info("Starting to upload result to {}...".format(new.location))
        
        with self.atomic(repo.engine):
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
                remote,
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
            repo_info = parse_repo(location)
            repo = Repository(namespace=repo_info.namespace, repository=repo_info.repository)
            remote = Repository.from_template(repo, engine=get_engine(repo_info.remote_name, autocommit=True))
 
            table_exists_at(remote, repo_info.table)
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
