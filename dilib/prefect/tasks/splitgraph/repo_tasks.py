from dataclasses import dataclass
from typing import Any, Dict, List, Tuple, Union

import pandas as pd
import prefect
from dilib.splitgraph import SchemaValidationError, RepoInfo, parse_repo
from prefect import Task
from prefect.utilities.collections import DotDict
from prefect.utilities.tasks import defaults_from_attrs
from semantic_version import NpmSpec, Version

from splitgraph.core.engine import get_engine, repository_exists
from splitgraph.core.repository import Repository, clone
from splitgraph.ingestion.pandas import df_to_table


def parse_tag(tag: str) -> Union[Version, None]:
    try:
        return Version(tag)
    except ValueError as exc:
        return None
class SemanticCheckoutTask(Task):
    """
    Clone a splitgraph repository using semantic version markers. The newest semantic tag is
    checked out based on the provided version markers.

    Args:

    Raises:


    Examples:

     ```python
    >>> SemanticCheckoutTask(RepoInfo(namespace='org1', repository='interesting_data'), major='1', minor='0') #None if no version is found
    None

    >>> SemanticCheckoutTask(RepoInfo(namespace='org1', repository='interesting_data'), major='1', minor='1') #Clones the newest tag based on major/minor
    Version('1.1.35')

    >>> SemanticCheckoutTask(RepoInfo(namespace='org1', repository='interesting_data'), major='1', minor='1', prerelease='hourly') #Clones the newest tag using prerelease to denote incremental updates
    Version('1.1.36-hourly.13')
    ```

    """


    def __init__(
      self,
      repo_info: RepoInfo,
      major: str = '1',
      minor: str = '0',
      prerelease: str = None,
      remote_name: str = None,
      **kwargs
    ) -> None:
        self.repo_info = repo_info
        self.major = major
        self.minor = minor
        self.prerelease = prerelease
        self.remote_name = remote_name
        super().__init__(**kwargs)


    @defaults_from_attrs('repo_info', 'major', 'minor', 'prerelease', 'remote_name')
    def run(self, repo_info: RepoInfo = None, major: str = None, minor: str = None, prerelease: str = None, remote_name: str = None, **kwargs: Any) -> Union[Version, None]:
        """

        Args:

        Returns:
            - Tuple of image_hash and semantic base_ref. If base_ref is None this means no semantic tags
            exist yet.
        """
        formatting_kwargs = {
            **kwargs,
            **prefect.context.get('parameters', {}).copy(),
            **prefect.context,
        }
        namespace = (repo_info.namespace or self.repo_info.namespace).format(**formatting_kwargs)
        repository = (repo_info.repository or self.repo_info.repository).format(**formatting_kwargs)

        major = major.format(**formatting_kwargs) if major else '1'
        minor = minor.format(**formatting_kwargs) if minor else '0'
        prerelease = prerelease.format(**formatting_kwargs) if prerelease else None
        remote_name = remote_name.format(**formatting_kwargs) if remote_name else None

        repo = Repository(namespace=namespace, repository=repository)

        if not repository_exists(repo):
            self.logger.info("Creating repo {}/{}...".format(repo.namespace, repo.repository))
            repo.init()

        if remote_name:
            remote = Repository.from_template(repo, engine=get_engine(remote_name, autocommit=True))
            cloned_repo=clone(
                remote,
                local_repository=repo,
                download_all=True,
                overwrite_objects=True,
                overwrite_tags=True,
            )

        image_tags = repo.get_all_hashes_tags()

        tag_dict = dict((tag, image_hash) for (image_hash, tag) in image_tags if image_hash) #reverse keys
        default_image = repo.images[tag_dict['latest']] if 'lastest' in tag_dict else repo.head
        version_list = [parse_tag(tag) for tag in tag_dict.keys()]

        valid_versions = [version for version in version_list if version]

        spec_expr = f'<={major}.{minor}' if minor else f'<={major}'
        base_ref_spec = NpmSpec(spec_expr)
        base_ref = base_ref_spec.select(valid_versions)

        if prerelease:
            assert base_ref, 'Cannot checkout using prerelease until a repo is initialized.'
            prerelease_base_version = base_ref.next_patch()
            base_ref = NpmSpec(f'>={str(prerelease_base_version)}-{prerelease}').select(valid_versions)

        image_hash = tag_dict[str(base_ref)] if base_ref else default_image.image_hash

        image = repo.images[image_hash]
        image.checkout(force=True)
        return base_ref

#
class CommitTask(Task):
    """
    Commit a splitgraph repository, optionally with a set of tags.

    Args:

    Raises:


    Examples:

     ```python
    >>> CommitTask(RepoInfo(namespace='org1', repository='interesting_data',  major='1', minor='0')) #None if no version is found
    None

    ```

    """


    def __init__(
      self,
      repo_info: RepoInfo,
      chunk_size: int = 10000,
      **kwargs
    ) -> None:
        self.repo_info = repo_info
        self.chunk_size = chunk_size
        super().__init__(**kwargs)


    @defaults_from_attrs('repo_info')
    def run(self, repo_info: RepoInfo = None, comment: str = None, tags: List[str] = [], **kwargs: Any):
        """

        Args:

        Returns:

        """
        formatting_kwargs = {
            **kwargs,
            **prefect.context.get('parameters', {}).copy(),
            **prefect.context,
        }
        namespace = (repo_info.namespace or self.repo_info.namespace).format(**formatting_kwargs)
        repository = (repo_info.repository or self.repo_info.repository).format(**formatting_kwargs)

        repo = Repository(namespace=namespace, repository=repository)
        new_img = repo.commit(comment=comment, chunk_size=self.chunk_size)
        for tag in tags:
            new_img.tag(tag)
        repo.engine.commit()


@dataclass(frozen=True)
class DataFrameToTableParams:
    data_frame: pd.DataFrame
    table: str = None
    if_exists: str = None

class DataFrameToTableTask(Task):
    """
    Use pandas dataframe to import data to table.

    Args:

    Raises:


    Examples:

     ```python
    >>> DataFrameToTableTask(RepoInfo(namespace='org1', repository='interesting_data',  major='1', minor='0')) #None if no version is found
    None

    ```

    """


    def __init__(
      self,
      repo_info: RepoInfo,
      table: str = None,
      if_exists: str = 'replace',
      schema_check: bool = False,
      **kwargs
    ) -> None:
        self.repo_info = repo_info
        self.table = table
        self.if_exists = if_exists
        self.schema_check = schema_check
        super().__init__(**kwargs)


    @defaults_from_attrs('table', 'if_exists', 'repo_info')
    def run(self, input: DataFrameToTableParams, table: str = None, if_exists: str = None, repo_info: RepoInfo = None, **kwargs: Any):
        """

        Args:

        Returns:

        """
        formatting_kwargs = {
            **kwargs,
            **prefect.context.get('parameters', {}).copy(),
            **prefect.context,
        }
        namespace = (repo_info.namespace or self.repo_info.namespace).format(**formatting_kwargs)
        repository = (repo_info.repository or self.repo_info.repository).format(**formatting_kwargs)
        table = table.format(**formatting_kwargs)

        repo = Repository(namespace=namespace, repository=repository)

        df_to_table(input.data_frame, repository=repo, table=input.table or table, if_exists=input.if_exists or if_exists)


class SemanticBumpTask(Task):
    """
    Given a semantic version, bumps the version using defined `semantic data` protocol.

    Args:

    Raises:


    Examples:

     ```python
    >>> SemanticBumpTask(RepoInfo(namespace='org1', repository='interesting_data',  major='1', minor='0')) #None if no version is found
    None

    ```

    """

    def __init__(
      self,
      build: Tuple[str] = ("{today}", "{task_run_id}"),
      **kwargs
    ) -> None:
        self.build = build
        super().__init__(**kwargs)


    @defaults_from_attrs('build')
    def run(self, base_ref: Version, build: Tuple[str] = None, **kwargs: Any) -> List[str]:
        """

        Args:

        Returns:

        """
        formatting_kwargs = {
            **kwargs,
            **prefect.context.get('parameters', {}).copy(),
            **prefect.context,
        }

        next_version = base_ref.next_patch() if base_ref else Version('1.0.0')
        is_prerelease = base_ref and len(base_ref.prerelease) >= 2
        if is_prerelease:
            prerelease, prerelease_count = base_ref.prerelease
            prerelease_count = int(prerelease_count)
            prerelease_count += 1
            next_version = Version(
                major=base_ref.major,
                minor=base_ref.minor,
                patch=base_ref.patch,
                prerelease=(prerelease, str(prerelease_count))
            )

        next_version.build = [meta.format(**formatting_kwargs) for meta in build]

        if is_prerelease:
            prerelease, prerelease_count = next_version.prerelease
            return [
                f'{next_version.major}-{prerelease}',
                f'{next_version.major}.{next_version.minor}-{prerelease}',
                str(next_version),
            ]

        return [
            f'{next_version.major}',
            f'{next_version.major}.{next_version.minor}',
            str(next_version),
        ]

class PushRepoTask(Task):
    """
    Push splitgraph changes to a remote engine.

    Args:

    Raises:


    Examples:

     ```python
    >>> PushRepoTask(RepoInfo(namespace='org1', repository='interesting_data'), remote_name='bedrock')


    ```

    """


    def __init__(
      self,
      repo_info: RepoInfo,
      remote_name: str = None,
      **kwargs
    ) -> None:
        self.repo_info = repo_info
        self.remote_name = remote_name
        super().__init__(**kwargs)


    @defaults_from_attrs('repo_info', 'remote_name')
    def run(self, repo_info: RepoInfo = None, remote_name: str = None, **kwargs: Any) -> Tuple[str, Union[str, None]]:
        """

        Args:

        Returns:

        """
        formatting_kwargs = {
            **kwargs,
            **prefect.context.get('parameters', {}).copy(),
            **prefect.context,
        }
        namespace = (repo_info.namespace or self.repo_info.namespace).format(**formatting_kwargs)
        repository = (repo_info.repository or self.repo_info.repository).format(**formatting_kwargs)

        remote_name = remote_name.format(**formatting_kwargs) if remote_name else None

        assert remote_name, 'Must specify a remote name to push'

        repo = Repository(namespace=namespace, repository=repository)
        remote = Repository.from_template(repo, engine=get_engine(remote_name, autocommit=True))
        repo.push(
            remote,
            handler="S3",
            overwrite_objects=True,
            overwrite_tags=True,
            reupload_objects=True,
        )
