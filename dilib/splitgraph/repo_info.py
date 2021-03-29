import re
from dataclasses import dataclass
from typing import TypedDict, Union, Dict, List
from urllib.parse import parse_qs, urlparse
from pathlib import PurePosixPath

from semantic_version import Version

repo_pattern = re.compile('(?P<namespace>.*)/(?P<repository>.*):(?P<tag>[\w.-]*)/?(?P<table>.*)')
tag_pattern = re.compile('(?P<major>\d*).?(?P<minor>\d*)-?(?P<prerelease>[\w.-]*)')




@dataclass(frozen=True)
class RepoInfo:
    uri: str
    remote_name: str
    namespace: str
    repository: str
    tag: str = None
    table: str = None
    major: str = '1'
    minor: str = None
    prerelease: str = None

    def v1_sgr_uri(self) -> str:
        return f'{self.namespace}/{self.repository}:{self.tag}'

class Workspace(TypedDict, total=False):
    repo_uri: str
    image_hash: str
    version: Version = None
    

def parse_tag(tag: str) -> Union[Version, None]:
    try:
        return Version(tag)
    except ValueError as exc:
        return None

def first_tag_in_params(params: Dict[str, List[str]]):
    if not 'tag' in params:
        return None
    
    tags = params['tag']

    return tags[0] if tags else None

def first_table_in_params(params: Dict[str, List[str]]):
    if not 'table' in params:
        return None
    
    tables = params['table']

    return tables[0] if tables else None

def parse_repo(uri: str) -> RepoInfo:
    parsed = urlparse(uri)
    segments = PurePosixPath(parsed.path)
    params = parse_qs(parsed.query)
 

    root, namesapce, repository = segments.parts
    tag = first_tag_in_params(params)
    data = dict(
        uri=uri,
        remote_name=parsed.netloc,
        namespace=namesapce,
        repository=repository,
        tag=tag,
        table=first_table_in_params(params),
    )


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

    return RepoInfo(**data)
