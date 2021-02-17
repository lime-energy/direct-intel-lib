import pandas as pd
import pendulum

from contextlib import contextmanager
from typing import Any, Dict

from prefect.engine.result import Result

from splitgraph.config.config import create_config_dict, patch_config
from splitgraph.core.engine import repository_exists, get_engine
from splitgraph.core.repository import Repository
from splitgraph.engine.postgres.engine import PostgresEngine
from splitgraph.ingestion.pandas import df_to_table


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
        - repo_name (str, optional): Splitgraph repository name
        - table (str, optional): Possibly templated table to be used for saving the result
            to the destination. Follows the logic of `.format(**kwargs)`.
        - comment (str, optional): Possibly templated table to be used for commenting the
            Splitgraph commit.
        - tag (str, optional): Possibly templated tag be used for tagging the Splitgraph
            commit.
        - env (Dict[str, Any], optional): Env vars to be patched to Splitgraph default
            configuration.
        - **kwargs (Any, optional): any additional `Result` initialization options
    """

    def __init__(
        self,
        namespace: str = None,
        repo_name: str = None,
        auto_init_repo: str = False,
        table: str = None,
        comment: str = None,
        tag: str = None,
        env: Dict[str, Any] = None,
        **kwargs: Any
    ) -> None:
        self.env = env or dict()
        self.namespace = namespace
        self.repo_name = repo_name
        self.auto_init_repo = auto_init_repo
        self.table = table
        self.comment = comment
        self.tag = tag

        super().__init__(**kwargs)

    def format(self, **kwargs: Any) -> "SplitgraphResult":
        """
        Takes a set of string format key-value pairs and renders the result.[table, comment, tag] to
        final strings

        Args:
            - **kwargs (Any): string format arguments for result.[table, comment, tag]

        Returns:
            - Result: a new result instance with the appropriately formatted table, comment, and tag
        """

        new = super().format(**kwargs)

        now = pendulum.now('utc')
        if isinstance(new.table, str):
            assert new.table is not None
            new.table = new.table.format(**kwargs)
        else:
            table = 'prefect-result' + now.format('Y-M-D-h-m-s')
            new.table = table

        if isinstance(new.comment, str):
            assert new.comment is not None
            new.comment = new.comment.format(**kwargs)
        else:
            new.comment = '(Auto) created at: ' + now.isoformat()

        if isinstance(new.tag, str):
            assert new.tag is not None
            new.tag = new.tag.format(**kwargs)
        else:
            new.tag = now.format('YYYYMMDDHHmmss')
        return new

    def read(self) -> Result:
        raise NotImplementedError

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

        cfg = patch_config(create_config_dict(), self.env or dict())
        engine = PostgresEngine(name='SplitgraphResult', conn_params=cfg)
        engine.initialize()
        repo = Repository(namespace=self.namespace, repository=self.repo_name, engine=engine)

        assert isinstance(value_, pd.DataFrame)
        assert engine.connected

        if not repository_exists(repo) and self.auto_init_repo:
            self.logger.info("Creating repo {}/{}...".format(repo.namespace, repo.repository))
            repo.init()

        # TODO: Retrieve the repo from bedrock first

        new = self.format(**kwargs)
        new.value = value_

        self.logger.info("Starting to upload result to {}...".format(new.table))

        with self.atomic(engine):
            self.logger.info("checkout")
            img = repo.head
            img.checkout(force=True)

            self.logger.info("df to table")
            df_to_table(new.value, repository=repo, table=new.table, if_exists='replace')

            self.logger.info("commit")
            new_img = repo.commit(comment=new.comment, chunk_size=10000)
            new_img.tag(new.tag)


        # if (repo.diff(new.table, img, new_img)):
        self.logger.info("push")
        repo.push(
            self.get_upstream(repo),
            handler="S3",
            overwrite_objects=True,
            overwrite_tags=True,
            reupload_objects=True,
        )

        engine.close()
        self.logger.info("Finished uploading result to {}...".format(new.table))

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

        raise NotImplementedError

    @contextmanager
    def atomic(self, engine):
        try:
            yield
        finally:
            self.logger.info("engine commit")
            engine.commit()

    def get_upstream(self, repository: Repository):
        return Repository.from_template(repository, engine=get_engine('bedrock', autocommit=True))

