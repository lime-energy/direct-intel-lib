import re
from typing import TypedDict

from dataclasses import dataclass
repo_pattern = re.compile('(?P<namespace>.*)/(?P<repository>.*):(?P<tag>.*)/(?P<table>.*)')

@dataclass(frozen=True)
class RepoInfo:
    namespace: str
    repository: str
    tag: str = None
    table: str = None

class RepoInfoDict(TypedDict, total=False):
    namespace: str
    repository: str
    tag: str = None
    table: str = None

@dataclass(frozen=True)
class SemanticInfo:
    major: str = '1'
    minor: str = None
    prerelease: str = None

class SemanticInfoDict(TypedDict, total=False):
    major: str = '1'
    minor: str = None
    prerelease: str = None

def parse_repo(uri: str) -> RepoInfo:
    data = repo_pattern.search(uri).groupdict()
    return RepoInfo(**data)
