import unittest

from .splitgraph_kubernetes_flow_config import CurrentContext


class FlowConfigTest(unittest.TestCase):
    def test_can_create(self):
        current_context = CurrentContext()

        self.assertIsNotNone(current_context)


if __name__ == '__main__':
    unittest.main()