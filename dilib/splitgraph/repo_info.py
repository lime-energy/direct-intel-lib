import re
from dataclasses import dataclass
from typing import TypedDict, Union

from semantic_version import Version

repo_pattern = re.compile('(?P<namespace>.*)/(?P<repository>.*):(?P<tag>[\w.-]*)/?(?P<table>.*)')
tag_pattern = re.compile('(?P<major>\d*).?(?P<minor>\d*)-?(?P<prerelease>[\w.-]*)')




@dataclass(frozen=True)
class RepoInfo:
    uri: str
    namespace: str
    repository: str
    tag: str = None
    table: str = None
    major: str = '1'
    minor: str = None
    prerelease: str = None

class Workspace(TypedDict, total=False):
    repo_info: RepoInfo
    image_hash: str
    version: Version = None
    
# class RepoInfoDict(TypedDict, total=False):
#     namespace: str
#     repository: str
#     tag: str
#     table: str
#     major: str
#     minor: str
#     prerelease: str



# @dataclass(frozen=True)
# class SemanticInfo:
#     major: str = '1'
#     minor: str = None
#     prerelease: str = None
# class SemanticInfoDict(TypedDict, total=False):
#     major: str
#     minor: str
#     prerelease: str

def parse_tag(tag: str) -> Union[Version, None]:
    try:
        return Version(tag)
    except ValueError as exc:
        return None

def parse_repo(uri: str) -> RepoInfo:
    data = repo_pattern.search(uri).groupdict()
    tag = data['tag']
    version = parse_tag(tag)
    if version:
        data['major'] = version.major
        data['minor'] = version.minor
        data['prerelease'] = version.prerelease
    else:
        tag_data = tag_pattern.search(tag).groupdict()
        data['major'] = tag_data['major']
        data['minor'] = tag_data['minor']
        data['prerelease'] = tag_data['prerelease']

    return RepoInfo(**data, uri=uri)
