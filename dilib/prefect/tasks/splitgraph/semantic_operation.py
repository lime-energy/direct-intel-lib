from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator

from prefect import Flow, Task, task

from .repo_tasks import (CommitTask, PushRepoTask, SemanticBumpTask,
                         SemanticCheckoutTask, SemanticCleanupTask,
                         version_formatter)

checkout = SemanticCheckoutTask()
commit = CommitTask()
sematic_bump = SemanticBumpTask()
push = PushRepoTask()
sematic_cleanup = SemanticCleanupTask()


@dataclass()
class SemanticOperation:
    workspaces: Task
    commit: Task = None
    push: Task = None
    cleanup: Task = None

@contextmanager
def semantic_operation(
    flow: Flow,
    upstream_repos: dict[str, str] = None,
    versions_to_retain = 1,
    remote_name: str = None,
) -> Iterator["SemanticOperation"]:
    workspaces = checkout(
        remote_name=remote_name,
        upstream_repos=upstream_repos,  
    )

    op = SemanticOperation(
        workspaces=workspaces
    )
  
    yield op

    repo_tags = sematic_bump(
        workspaces=workspaces,
    )

    changes_repo_uris = commit(
        tags=repo_tags, 
        workspaces=workspaces,
        upstream_tasks=flow.terminal_tasks(),
    )

    pushed = push(
        repo_uris=changes_repo_uris,
        remote_name=remote_name
    )
    cleanup=sematic_cleanup(
        retain=versions_to_retain, 
        repo_uris=changes_repo_uris,
        remote_name=remote_name,
        upstream_tasks=[pushed]
    )

       
    op.commit=changes_repo_uris
    op.push=pushed
    op.cleanup=cleanup
