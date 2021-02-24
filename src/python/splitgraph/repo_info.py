import re
repo_pattern = re.compile('(?P<namespace>.*)/(?P<repo>.*):(?P<tag>.*)/(?P<table>.*)')

def parse_repo(uri: str) -> dict:
    return repo_pattern.search(uri).groupdict()
