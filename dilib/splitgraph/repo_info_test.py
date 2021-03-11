import unittest

from .repo_info import parse_repo, RepoInfo


class RepoInfoTest(unittest.TestCase):

    def test_can_parse_full_uri(self):
        info = parse_repo('sgr://bedrock/namespace/repo?tag=1.1&table=table')
        self.assertEqual(RepoInfo(uri='sgr://bedrock/namespace/repo?tag=1.1&table=table', remote_name='bedrock', namespace='namespace', repository='repo', tag='1.1', table='table', major='1', minor='1', prerelease=''), info)

if __name__ == '__main__':
    unittest.main()