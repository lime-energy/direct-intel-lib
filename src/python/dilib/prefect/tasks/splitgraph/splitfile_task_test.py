import unittest
import prefect
import pkgutil

from prefect.engine import TaskRunner

from .splitfile_task import SplitfileTask



class SplitfileTaskTest(unittest.TestCase):

    def test_can_build_splitfile(self):
        splitfile = pkgutil.get_data(__name__, "example.splitfile").decode("utf-8")
        splitfile_task = SplitfileTask(
            uri="integration-tests/splitfile-test:{today_nodash}/table", 
            splitfile_commands=splitfile,
        ) 
        runner = TaskRunner(task=splitfile_task)

        with prefect.context(today_nodash="20210226"):
            state = runner.run()
            self.assertTrue(state.is_successful())


if __name__ == '__main__':
    unittest.main()