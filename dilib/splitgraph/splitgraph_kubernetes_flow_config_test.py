import unittest

from .splitgraph_kubernetes_flow_config import CurrentContext


class FlowConfigTest(unittest.TestCase):
    def test_can_create(self):
        current_context = CurrentContext()

        self.assertIsNotNone(current_context)
    def test_can_unpack_config(self):
        current_context = CurrentContext()
        flow_config, flow_reg = current_context.build_standard_config('test', 'path', env={})
        self.assertIsNotNone(flow_config)
        self.assertIsNotNone(flow_reg)


if __name__ == '__main__':
    unittest.main()