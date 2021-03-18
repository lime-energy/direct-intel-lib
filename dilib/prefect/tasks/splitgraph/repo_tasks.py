
from dataclasses import dataclass
from typing import Any, Dict, List, NamedTuple, Tuple, TypedDict, Union

import pandas as pd
import pendulum
import prefect
from dilib.format import format_with_default
from dilib.splitgraph import (RepoInfo, SchemaValidationError, Workspace,
                              parse_repo, parse_tag)
from prefect import Task
from prefect.tasks.templates.strings import StringFormatter
from prefect.utilities.collections import DotDict
from prefect.utilities.tasks import defaults_from_attrs
from semantic_version import NpmSpec, Version

from splitgraph.core.engine import get_engine, repository_exists
from splitgraph.core.repository import Repository, clone
from splitgraph.ingestion.pandas import df_to_table

version_formatter = StringFormatter(name='semantic version formatter', template='{major}.{minor}.{patch}')





    
class SemanticCheckoutTask(Task):
    """
    Clone a splitgraph repository using semantic version markers. The newest semantic tag is
    checked out based on the provided version markers.

    Args:

    Raises:


    Examples:

     ```python
    >>> SemanticCheckoutTask(upstream_repos=dict(test_repo: 'org1/interesting_data:1.1')) #None if no version is found
    None

    >>> SemanticCheckoutTask(upstream_repos=dict(test_repo: 'org1/interesting_data:1.1')) #Clones the newest tag based on major/minor
    Version('1.1.35')

    >>> SemanticCheckoutTask(upstream_repos=dict(test_repo: 'org1/interesting_data:1.1-hourly')) #Clones the newest tag using prerelease to denote incremental updates
    Version('1.1.36-hourly.13')
    ```

    """


    def __init__(
      self,
      upstream_repos: dict[str, str] = None,
      remote_name: str = None,
      **kwargs
    ) -> None:
        self.upstream_repos = upstream_repos
        self.remote_name = remote_name
        super().__init__(**kwargs)


    @defaults_from_attrs('upstream_repos', 'remote_name')
    def run(self, upstream_repos: dict[str, str] = None, remote_name: str = None, **kwargs: Any) -> dict[str, Workspace]:
        """

        Args:

        Returns:
            - 
        """
        repo_infos = dict((name, parse_repo(uri)) for (name, uri) in upstream_repos.items())
        repos = dict((name, self.init_repo(repo_info, remote_name)) for (name, repo_info) in repo_infos.items())
      
        workspaces = dict((name, self.checkout_workspace(repo, repo_infos[name])) for (name, repo) in repos.items())

        return workspaces
 

    
    def init_repo(self, repo_info: RepoInfo, remote_name: str) -> Repository:
        repo = Repository(namespace=repo_info.namespace, repository=repo_info.repository)

        if not repository_exists(repo):
            self.logger.info("Creating repo {}/{}...".format(repo.namespace, repo.repository))
            repo.init()

        if remote_name:
            remote = Repository.from_template(repo, engine=get_engine(remote_name, autocommit=True))
            cloned_repo=clone(
                remote,
                local_repository=repo,
                download_all=False,
                overwrite_objects=True,
                overwrite_tags=True,
            )
        return repo

    def checkout_workspace(self, repo: Repository, repo_info: RepoInfo) -> Workspace:
        image_tags = repo.get_all_hashes_tags()

        tag_dict = dict((tag, image_hash) for (image_hash, tag) in image_tags if image_hash) #reverse keys
        default_image = repo.images[tag_dict['latest']] if 'lastest' in tag_dict else repo.head
        version_list = [parse_tag(tag) for tag in sorted(list(tag_dict.keys()), key=len, reverse=True)]

        valid_versions = [version for version in version_list if version]

        spec_expr = f'<={repo_info.major}.{repo_info.minor}' if repo_info.minor else f'<={repo_info.major}'
        base_ref_spec = NpmSpec(spec_expr)
        base_ref = base_ref_spec.select(valid_versions)

        if repo_info.prerelease:
            assert base_ref, 'Cannot checkout using prerelease until a repo is initialized.'
            prerelease_base_version = base_ref.next_patch()
            base_ref = NpmSpec(f'>={str(prerelease_base_version)}-{repo_info.prerelease}').select(valid_versions)

        image_hash = tag_dict[str(base_ref)] if base_ref else default_image.image_hash

        image = repo.images[image_hash]
        image.checkout(force=True)
        return Workspace(repo_info=repo_info, image_hash=image_hash, version=base_ref)
class SemanticCleanupTask(Task):
    """
    Remove old tags. The retain parameter prevents this number of the newest tags from being removed.

    Args:

    Raises:


    Examples:

     ```python
    >>> SemanticCleanupTask(dict(repo1='org1/interesting_data:1'), retain=3) #Retain 3 most recent tags

    >>> SemanticCheckoutTask(dict(repo1='org1/interesting_data:1-hourly'), retain=48) #Retain 48 most recent hourly tags

    ```

    """


    def __init__(
      self,
      repo_uris: dict[str, str] = None,
      remote_name: str = None,
      retain: int = 1,
      **kwargs
    ) -> None:
        self.repo_uris = repo_uris
        self.remote_name = remote_name
        self.retain = retain
        super().__init__(**kwargs)


    @defaults_from_attrs('repo_uris', 'remote_name', 'retain')
    def run(self, repo_uris: dict[str, str] = None, remote_name: str = None, retain: int = None, **kwargs: Any) -> Union[Version, None]:
        """

        Args:

        Returns:

        """


        repo_infos = dict((name, parse_repo(uri)) for (name, uri) in repo_uris.items())
        repos = dict((name, Repository(namespace=repo_info.namespace, repository=repo_info.repository)) for (name, repo_info) in repo_infos.items())

        repos_to_prune = dict((name, Repository.from_template(repo, engine=get_engine(remote_name, autocommit=True))) for (name, repo) in repos.items()) if remote_name else repos

        for name, repo_info in repo_infos.items():
            repo = repos_to_prune[name]
            prerelease = repo_info.prerelease
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
    >>> CommitTask(tags=tags)
    None

    ```

    """


    def __init__(
      self,
      workspaces: dict[str, Workspace] = None,
      tags: dict[str, List[str]] = None,
      chunk_size: int = 10000,
      **kwargs
    ) -> None:
        self.workspaces = workspaces
        self.tags = tags
        self.chunk_size = chunk_size
        super().__init__(**kwargs)


    @defaults_from_attrs('workspaces', 'tags')
    def run(self, workspaces: dict[str, Workspace] = None, comment: str = None, tags: dict[str, List[str]] = None, **kwargs: Any):
        """

        Args:

        Returns:

        """

        repos = dict((name, Repository(namespace=workspace['repo_info'].namespace, repository=workspace['repo_info'].repository)) for (name, workspace) in workspaces.items())
        repos_with_changes = dict((name, repo) for (name, repo) in repos.items() if repo.has_pending_changes())

        
        for name, repo in repos_with_changes.items():
            repo_tags = tags[name] if name in tags else []
            new_img = repo.commit(comment=comment, chunk_size=self.chunk_size)
            for tag in repo_tags:
                new_img.tag(tag)
            repo.engine.commit()
    
        changes_repo_uris = dict((name, workspaces[name]['repo_info'].uri) for (name, repo) in repos_with_changes.items())
        return changes_repo_uris

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
    >>> DataFrameToTableTask('org1/interesting_data:1')
    None

    ```

    """


    def __init__(
      self,
      repo_uri: str = None,
      table: str = None,
      if_exists: str = 'replace',
      schema_check: bool = False,
      **kwargs
    ) -> None:
        self.repo_uri = repo_uri
        self.table = table
        self.if_exists = if_exists
        self.schema_check = schema_check
        super().__init__(**kwargs)


    @defaults_from_attrs('table', 'if_exists', 'repo_uri')
    def run(self, params: DataFrameToTableParams, table: str = None, if_exists: str = None, repo_uri: str = None, **kwargs: Any):
        """

        Args:

        Returns:

        """
        assert repo_uri, 'Must specify repo_uri.'
        repo_info = parse_repo(repo_uri)

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

    Example outdated

     ```python
    >>> bump=SemanticBumpTask(build=('meta1', 'meta2'))
    >>> tags=bump(base_ref=version=Version('1.0.0'))
    Result(value=['1', '1.0', '1.0.1+meta1.meta2'])


    ```

    """

    def __init__(
        self,
        workspaces: dict[str, Workspace] = None,
        build: Tuple[str] = ("{date:%Y-%m-%dT%H}", "{date:%M}", "{flow_run_name}"),
        **kwargs
    ) -> None:
        self.workspaces = workspaces
        self.build = build
        super().__init__(**kwargs)


    @defaults_from_attrs('workspaces', 'build')
    def run(self, workspaces: dict[str, Workspace] = None, build: Tuple[str] = None, **kwargs: Any) -> dict[str, List[str]]:
        """

        Args:

        Returns:

        """

        repo_tags = dict((name, self.workspace_tags(workspace, build, **kwargs)) for (name, workspace) in workspaces.items())
        return repo_tags

    def workspace_tags(self, workspace: Workspace, build: Tuple[str], **kwargs: Any) -> List[str]:
        formatting_kwargs = {
            **kwargs,
            **prefect.context.get('parameters', {}).copy(),
            **prefect.context,
        }

        base_ref = workspace['version']
       
        next_version = base_ref.next_patch() if base_ref else Version(f'{workspace["repo_info"].major}.{workspace["repo_info"].minor}.0' if workspace["repo_info"].minor else f'{workspace["repo_info"].major}.0.0')
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
    >>> PushRepoTask(dict(repo1='org1/interesting_data:1'), remote_name='bedrock')


    ```

    """


    def __init__(
      self,
      repo_uris: dict[str, str] = None,
      remote_name: str = None,
      **kwargs
    ) -> None:
        self.repo_uris = repo_uris
        self.remote_name = remote_name
        super().__init__(**kwargs)


    @defaults_from_attrs('repo_uris', 'remote_name')
    def run(self, repo_uris: dict[str, str] = None, remote_name: str = None, **kwargs: Any):
        """

        Args:

        Returns:

        """
        if not remote_name:
            self.logger.warn('No remote_name specified. Not pushing.')
            return

        repo_infos = dict((name, parse_repo(uri)) for (name, uri) in repo_uris.items())
        repos = dict((name, Repository(namespace=repo_info.namespace, repository=repo_info.repository)) for (name, repo_info) in repo_infos.items())

        for name, repo in repos.items():
            remote = Repository.from_template(repo, engine=get_engine(remote_name, autocommit=True))
            repo.push(
                remote,
                handler="S3",
                overwrite_objects=True,
                overwrite_tags=True,
                reupload_objects=True,
            )
