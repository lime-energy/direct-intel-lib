import unittest
from pathlib import Path
from .current_context import CurrentContext


class FlowConfigTest(unittest.TestCase):
    def test_can_create(self):
        current_context = CurrentContext(project_path=Path.cwd() / 'fake_proj')

        self.assertIsNotNone(current_context)
    def test_can_unpack_config(self):
        current_context = CurrentContext(project_path=Path.cwd() / 'fake_proj')
        flow_config, flow_reg = current_context.build_standard_config('test', 'path', env={})
        self.assertIsNotNone(flow_config)
        self.assertIsNotNone(flow_reg)

    def test_can_create_catalog(self):
        current_context = CurrentContext(project_path=Path.cwd() / 'fake_proj')
        print(Path.cwd())
        catalog = current_context.catalog

        self.assertIsNotNone(catalog)
        df = catalog.load('example_iris_data')
        self.assertIsNotNone(df)

        ds = getattr(catalog.datasets, 'example_iris_data')
        self.assertIsNotNone(ds)

        names = catalog.list('iris')
        self.assertEqual(names, ['example_iris_data'])

        print(type(self))
        print(current_context.params)

if __name__ == '__main__':
    unittest.main()