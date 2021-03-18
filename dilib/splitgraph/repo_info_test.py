import unittest

from .repo_info import parse_repo, RepoInfo


class RepoInfoTest(unittest.TestCase):

    def test_can_parse_full_uri(self):
        info = parse_repo('namespace/repo:1.1/table')
        self.assertEqual(RepoInfo(uri='namespace/repo:1.1/table', namespace='namespace', repository='repo', tag='1.1', table='table', major='1', minor='1', prerelease=''), info)

if __name__ == '__main__':
    unittest.main()