import pkgutil
import unittest

import prefect
from dilib.splitgraph import RepoInfo, Workspace
from prefect.engine import TaskRunner
from prefect.utilities.debug import raise_on_exception

from .splitfile_task import SplitfileTask


class SplitfileTaskTest(unittest.TestCase):

    def test_can_build_splitfile(self):
        splitfile = pkgutil.get_data(__package__, "example.splitfile").decode("utf-8")
        splitfile_task = SplitfileTask(
            upstream_repos=dict(
                test='integration-tests/splitfile-test:1'
            ),
            output=Workspace(
                repo_uri='integration-tests/splitfile-test:1',
                image_hash=None,
            ),
            splitfile_commands=splitfile,
        ) 
        runner = TaskRunner(task=splitfile_task)

        with raise_on_exception():
            with prefect.context(today_nodash="20210226"):
                state = runner.run()
                self.assertTrue(state.is_successful())


if __name__ == '__main__':
    unittest.main()
