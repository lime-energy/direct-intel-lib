import unittest

import prefect
from prefect.engine import TaskRunner

from .splitgraph_kubernetes_run import SplitgraphKubernetesRun


class SplitfileTaskTest(unittest.TestCase):

    def test_can_build_create_config(self):
        config = SplitgraphKubernetesRun(image='myimg')
        self.assertTrue(config)


if __name__ == '__main__':
    unittest.main()
