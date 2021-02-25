import re
repo_pattern = re.compile('(?P<namespace>.*)/(?P<repo>.*):(?P<tag>.*)/(?P<table>.*)')

def parse_repo(uri: str):
    return repo_pattern.search(uri).groupdict()
