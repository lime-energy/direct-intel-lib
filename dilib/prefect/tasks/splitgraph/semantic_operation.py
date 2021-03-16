from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator

from dilib.splitgraph import RepoInfoDict, SemanticInfoDict
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
    workspace: Task
    commit: Task = None
    push: Task = None
    cleanup: Task = None

@contextmanager
def semantic_operation(
    flow: Flow,
    repo_dict: RepoInfoDict = None,
    semantic_dict: SemanticInfoDict = dict(
        major='1'
    ),
    versions_to_retain = 1,
    remote_name: str = None,
) -> Iterator["SemanticOperation"]:
    workspace = checkout(
        remote_name=remote_name,
        repo_dict=repo_dict,   
        semantic_dict=semantic_dict,      
    )

    op = SemanticOperation(
        workspace=workspace
    )
  
    yield op

    initial_version = version_formatter(
        major=semantic_dict['major'],
        minor=semantic_dict['minor'],
        patch=0
    )
    tags = sematic_bump(
        initial_version=initial_version,
        base_ref=workspace['version'],
    )

    commit_done = commit(
        tags=tags, 
        repo_dict=workspace['repo_dict'],
        upstream_tasks=flow.terminal_tasks(),
    )

    pushed = push(
        repo_dict=workspace['repo_dict'],
        remote_name=workspace['remote_name'],
        upstream_tasks=[commit_done]
    )
    cleanup=sematic_cleanup(
        retain=versions_to_retain, 
        repo_dict=workspace['repo_dict'],
        remote_name=workspace['remote_name'],
        prerelease=semantic_dict['prerelease'],
        upstream_tasks=[pushed]
    )

       
    op.commit=commit_done
    op.push=pushed
    op.cleanup=cleanup
