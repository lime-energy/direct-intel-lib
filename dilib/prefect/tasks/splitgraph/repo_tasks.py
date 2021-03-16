
from dataclasses import dataclass
from typing import Any, Dict, List, NamedTuple, Tuple, TypedDict, Union

import pandas as pd
import pendulum
import prefect
from dilib.format import format_with_default
from dilib.splitgraph import (RepoInfo, RepoInfoDict, SchemaValidationError,
                              SemanticInfo, SemanticInfoDict, parse_repo)
from prefect import Task
from prefect.tasks.templates.strings import StringFormatter
from prefect.utilities.collections import DotDict
from prefect.utilities.tasks import defaults_from_attrs
from semantic_version import NpmSpec, Version

from splitgraph.core.engine import get_engine, repository_exists
from splitgraph.core.repository import Repository, clone
from splitgraph.ingestion.pandas import df_to_table

version_formatter = StringFormatter(name='semantic version formatter', template='{major}.{minor}.{patch}')


def parse_tag(tag: str) -> Union[Version, None]:
    try:
        return Version(tag)
    except ValueError as exc:
        return None

class Workspace(TypedDict, total=False):
    repo_dict: RepoInfoDict
    image_hash: str
    version: Version = None
    remote_name: str = None
    
class SemanticCheckoutTask(Task):
    """
    Clone a splitgraph repository using semantic version markers. The newest semantic tag is
    checked out based on the provided version markers.

    Args:

    Raises:


    Examples:

     ```python
    >>> SemanticCheckoutTask(RepoInfoDict(namespace='org1', repository='interesting_data'), major='1', minor='0') #None if no version is found
    None

    >>> SemanticCheckoutTask(RepoInfoDict(namespace='org1', repository='interesting_data'), major='1', minor='1') #Clones the newest tag based on major/minor
    Version('1.1.35')

    >>> SemanticCheckoutTask(RepoInfoDict(namespace='org1', repository='interesting_data'), major='1', minor='1', prerelease='hourly') #Clones the newest tag using prerelease to denote incremental updates
    Version('1.1.36-hourly.13')
    ```

    """


    def __init__(
      self,
      repo_dict: RepoInfoDict = None,
      semantic_dict: SemanticInfoDict = dict(
          major='1',
      ),
      remote_name: str = None,
      **kwargs
    ) -> None:
        self.repo_dict = repo_dict
        self.semantic_dict = semantic_dict
        self.remote_name = remote_name
        super().__init__(**kwargs)


    @defaults_from_attrs('repo_dict', 'semantic_dict', 'remote_name')
    def run(self, repo_dict: RepoInfoDict = None, semantic_dict: SemanticInfoDict = None, remote_name: str = None, **kwargs: Any) -> Workspace:
        """

        Args:

        Returns:
            - Tuple of image_hash and semantic base_ref. If base_ref is None this means no semantic tags
            exist yet.
        """
        assert repo_dict, 'Must specify repo.'
        repo_info = RepoInfo(**repo_dict)
        semantic_info = SemanticInfo(**semantic_dict)


        repo = Repository(namespace=repo_info.namespace, repository=repo_info.repository)

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
        version_list = [parse_tag(tag) for tag in sorted(list(tag_dict.keys()), key=len, reverse=True)]

        valid_versions = [version for version in version_list if version]

        spec_expr = f'<={semantic_info.major}.{semantic_info.minor}' if semantic_info.minor else f'<={semantic_info.major}'
        base_ref_spec = NpmSpec(spec_expr)
        base_ref = base_ref_spec.select(valid_versions)

        if semantic_info.prerelease:
            assert base_ref, 'Cannot checkout using prerelease until a repo is initialized.'
            prerelease_base_version = base_ref.next_patch()
            base_ref = NpmSpec(f'>={str(prerelease_base_version)}-{semantic_info.prerelease}').select(valid_versions)

        image_hash = tag_dict[str(base_ref)] if base_ref else default_image.image_hash

        image = repo.images[image_hash]
        image.checkout(force=True)
        return Workspace(repo_dict=repo_dict, image_hash=image_hash, version=base_ref, remote_name=remote_name)

class SemanticCleanupTask(Task):
    """
    Remove old tags. The retain parameter prevents this number of the newest tags from being removed.

    Args:

    Raises:


    Examples:

     ```python
    >>> SemanticCleanupTask(RepoInfoDict(namespace='org1', repository='interesting_data'), retain=3) #Retain 3 most recent tags

    >>> SemanticCheckoutTask(RepoInfoDict(namespace='org1', repository='interesting_data'), prerelease='hourly', retain=48) #Retain 48 most recent hourly tags

    ```

    """


    def __init__(
      self,
      repo_dict: RepoInfoDict = None,
      prerelease: str = None,
      remote_name: str = None,
      retain: int = 1,
      **kwargs
    ) -> None:
        self.repo_dict = repo_dict
        self.prerelease = prerelease
        self.remote_name = remote_name
        self.retain = retain
        super().__init__(**kwargs)


    @defaults_from_attrs('repo_dict', 'prerelease', 'remote_name', 'retain')
    def run(self, repo_dict: RepoInfoDict = None, prerelease: str = None, remote_name: str = None, retain: int = None, **kwargs: Any) -> Union[Version, None]:
        """

        Args:

        Returns:

        """
        assert repo_dict, 'Must specify repo.'
        repo_info = RepoInfo(**repo_dict)

        repo = Repository(namespace=repo_info.namespace, repository=repo_info.repository)

        if remote_name:
            repo = Repository.from_template(repo, engine=get_engine(remote_name, autocommit=True))

        image_tags = repo.get_all_hashes_tags()

        tag_dict = dict((tag, image_hash) for (image_hash, tag) in image_tags if image_hash) #reverse keys

        version_list = [parse_tag(tag) for tag in sorted(list(tag_dict.keys()), key=len, reverse=True)]

        valid_versions = [version for version in version_list if version]
        non_prerelease_versions = [version for version in valid_versions if len(version.prerelease) == 0]
        prerelease_versions = [version for version in valid_versions if prerelease and len(version.prerelease) > 0 and version.prerelease[0] == prerelease]
        prune_candidates = prerelease_versions if prerelease else non_prerelease_versions

        total_candidates = len(prune_candidates)
        prune_count = total_candidates - retain
        prune_list = sorted(prune_candidates)[:prune_count]

        for version in prune_list:
            tag = str(version)
            image_hash = tag_dict[tag]
            image = repo.images[image_hash]
            image.delete_tag(tag)


class VersionToDateTask(Task):
    """
    Parses the date from a version build meta data

    Args:

    Raises:


    Examples:


    """
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def run(self, version: Version, **kwargs: Any) -> Union[Version, None]:
        """

        Args:

        Returns:

        """

        if not version:
            return None

        if len(version.build) < 1:
            return None

        date_str = version.build[0]

        date = pendulum.parse(date_str)
        if len(version.build) < 2:
            return date

        try:
            minutes = version.build[1]
            return date.add(minutes=int(minutes))
        except Exception as ex:
            return date


#
class CommitTask(Task):
    """
    Commit a splitgraph repository, optionally with a set of tags.

    Args:

    Raises:


    Examples:

     ```python
    >>> CommitTask(repo_info=repo_info, tags=tags)
    None

    ```

    """


    def __init__(
      self,
      repo_dict: RepoInfoDict = None,
      chunk_size: int = 10000,
      **kwargs
    ) -> None:
        self.repo_dict = repo_dict
        self.chunk_size = chunk_size
        super().__init__(**kwargs)


    @defaults_from_attrs('repo_dict')
    def run(self, repo_dict: RepoInfoDict = None, comment: str = None, tags: List[str] = [], **kwargs: Any):
        """

        Args:

        Returns:

        """
        
        assert repo_dict, 'Must specify repo_dict.'
        repo_info = RepoInfo(**repo_dict)

        repo = Repository(namespace=repo_info.namespace, repository=repo_info.repository)
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
    >>> DataFrameToTableTask(RepoInfoDict(namespace='org1', repository='interesting_data'))
    None

    ```

    """


    def __init__(
      self,
      repo_dict: RepoInfoDict = None,
      table: str = None,
      if_exists: str = 'replace',
      schema_check: bool = False,
      **kwargs
    ) -> None:
        self.repo_dict = repo_dict
        self.table = table
        self.if_exists = if_exists
        self.schema_check = schema_check
        super().__init__(**kwargs)


    @defaults_from_attrs('table', 'if_exists', 'repo_dict')
    def run(self, params: DataFrameToTableParams, table: str = None, if_exists: str = None, repo_dict: RepoInfoDict = None, **kwargs: Any):
        """

        Args:

        Returns:

        """
        assert repo_dict, 'Must specify repo_dict.'
        repo_info = RepoInfo(**repo_dict)

        repo = Repository(namespace=repo_info.namespace, repository=repo_info.repository)

        table = params.table or table
        if_exists = params.if_exists or if_exists


        df_to_table(params.data_frame, repository=repo, table=table, if_exists=if_exists)



class SemanticBumpTask(Task):
    """
    Given a semantic version, bumps the version using defined `semantic data` protocol.

    Args:

    Raises:


    Examples:

     ```python
    >>> bump=SemanticBumpTask(build=('meta1', 'meta2'))
    >>> tags=bump(base_ref=version=Version('1.0.0'))
    Result(value=['1', '1.0', '1.0.1+meta1.meta2'])


    ```

    """

    def __init__(
        self,
        initial_version: str = None,
        build: Tuple[str] = ("{date:%Y-%m-%dT%H}", "{date:%M}", "{flow_run_name}"),
        **kwargs
    ) -> None:
        self.build = build
        self.initial_version = initial_version
        super().__init__(**kwargs)


    @defaults_from_attrs('initial_version', 'build')
    def run(self, base_ref: Version, initial_version: str = None, build: Tuple[str] = None, **kwargs: Any) -> List[str]:
        """

        Args:

        Returns:

        """
        formatting_kwargs = {
            **kwargs,
            **prefect.context.get('parameters', {}).copy(),
            **prefect.context,
        }

        next_version = base_ref.next_patch() if base_ref else Version(initial_version or '1.0.0')
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
    >>> PushRepoTask(RepoInfoDict(namespace='org1', repository='interesting_data'), remote_name='bedrock')


    ```

    """


    def __init__(
      self,
      repo_dict: RepoInfoDict = None,
      remote_name: str = None,
      **kwargs
    ) -> None:
        self.repo_dict = repo_dict
        self.remote_name = remote_name
        super().__init__(**kwargs)


    @defaults_from_attrs('repo_dict', 'remote_name')
    def run(self, repo_dict: RepoInfoDict = None, remote_name: str = None, **kwargs: Any) -> Tuple[str, Union[str, None]]:
        """

        Args:

        Returns:

        """
        assert repo_dict, 'Must specify repo.'
        repo_info = RepoInfo(**repo_dict)

       
        if not remote_name:
            self.logger.warn('No remote_name specified. Not pushing.')
            return

        repo = Repository(namespace=repo_info.namespace, repository=repo_info.repository)

        remote = Repository.from_template(repo, engine=get_engine(remote_name, autocommit=True))
        repo.push(
            remote,
            handler="S3",
            overwrite_objects=True,
            overwrite_tags=True,
            reupload_objects=True,
        )
