import re

from dataclasses import dataclass
repo_pattern = re.compile('(?P<namespace>.*)/(?P<repository>.*):(?P<tag>.*)/(?P<table>.*)')

@dataclass(frozen=True)
class RepoInfo:
    namespace: str
    repository: str
    tag: str = None
    table: str = None

def parse_repo(uri: str) -> RepoInfo:
    data = repo_pattern.search(uri).groupdict()
    return RepoInfo(**data)
