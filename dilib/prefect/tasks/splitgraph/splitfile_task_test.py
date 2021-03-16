import unittest
import prefect
import pkgutil

from prefect.engine import TaskRunner

from .splitfile_task import SplitfileTask



class SplitfileTaskTest(unittest.TestCase):

    def test_can_build_splitfile(self):
        splitfile = pkgutil.get_data(__package__, "example.splitfile").decode("utf-8")
        splitfile_task = SplitfileTask(
            repo_dict=dict(
                namespace='integration-tests',
                repository='splitfile-test'
            ),
            splitfile_commands=splitfile,
        ) 
        runner = TaskRunner(task=splitfile_task)

        with prefect.context(today_nodash="20210226"):
            state = runner.run()
            self.assertTrue(state.is_successful())


if __name__ == '__main__':
    unittest.main()