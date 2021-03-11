from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator, Dict

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
    upstream_repos: Dict[str, str] = None,
    versions_to_retain = 1,
) -> Iterator["SemanticOperation"]:
    workspaces = checkout(
        upstream_repos=upstream_repos,  
    )

    op = SemanticOperation(
        workspaces=workspaces
    )
  
    yield op

    repo_tags = sematic_bump(
        workspaces=workspaces,
    )

    committed_repo_uris = commit(
        workspaces=workspaces,
        upstream_tasks=flow.terminal_tasks(),
    )

    tagged_repo_uris = push(
        sgr_tags=repo_tags, 
        workspaces=workspaces,
        upstream_tasks=[committed_repo_uris],
    )
    cleanup=sematic_cleanup(
        retain=versions_to_retain, 
        repo_uris=tagged_repo_uris,
    )

       
    op.commit=committed_repo_uris
    op.push=tagged_repo_uris
    op.cleanup=cleanup
