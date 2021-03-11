import unittest

from .repo_info import parse_repo, RepoInfo


class RepoInfoTest(unittest.TestCase):

    def test_can_parse_full_uri(self):
        info = parse_repo("namespace/repo:tag/table")
        self.assertEqual(RepoInfo("namespace", "repo", "tag", "table"), info)

if __name__ == '__main__':
    unittest.main()