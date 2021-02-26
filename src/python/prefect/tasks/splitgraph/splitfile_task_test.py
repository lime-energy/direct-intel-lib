import unittest
import prefect
from pkg_resources import resource_listdir, resource_string
from prefect.engine import TaskRunner

from .splitfile_task import SplitfileTask




class SplitfileTaskTest(unittest.TestCase):

    def test_can_build_splitfile(self):
        splitfile = resource_string("src.python.prefect.tasks.splitgraph", "example.splitfile").decode("utf-8")
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