import datetime
import unittest
import pandas as pd
import numpy as np
import pendulum
import prefect
from pkg_resources import resource_listdir, resource_string
from prefect import Flow, task
from prefect.engine import TaskRunner
import logging
from src.python.prefect.engine.results import SplitgraphResult
logging.info('test')
result=SplitgraphResult(location="test1/test1:{today_nodash}/test_abc", remote_name='bedrock', auto_init_repo=True)
@task(result=result, checkpoint=True)
def test_task():
    index = pd.date_range("1/1/2000", periods=8)
    s = pd.Series(np.random.randn(5), index=["a", "b", "c", "d", "e"])
    df = pd.DataFrame(np.random.randn(8, 3), index=index, columns=["A", "B", "C"])    
    return df  

with Flow("test") as flow:
    r1=test_task()
class SplitgraphResultTest(unittest.TestCase):

    # def test_arr(self):
    #     index = pd.date_range("1/1/2000", periods=8)
    #     s = pd.Series(np.random.randn(5), index=["a", "b", "c", "d", "e"])
    #     df = pd.DataFrame(np.random.randn(8, 3), index=index, columns=["A", "B", "C"])    
    #     recs = df.to_records()
    #     print(recs[0])
    #     print(recs.dtype)
    #     print(df.to_dict(orient='records'))
    #     n1 = pd.DataFrame.from_records([recs[0]])
    #     print(n1)
    #     self.assertFalse(recs)
    def test_can_write_result(self):
        # self.assertFalse(result.location)

        with prefect.context(today_nodash="x1"):
            state = flow.run()
            print(state.message)
            print(state.result[r1])
            for task in flow.tasks:
                print(state.result[task])
            self.assertTrue(state.is_successful())


if __name__ == '__main__':
    unittest.main()
