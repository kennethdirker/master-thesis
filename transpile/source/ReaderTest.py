from pathlib import Path
import unittest
from Reader import Reader
from Datastructures import Graph

class ReaderTesting(unittest.TestCase):
    def test_0_reader_step_graph(self):
        cwl_test_file_path = Path("../cwl_examples/steps/echo.cwl")
        reader = Reader()
        graph = reader.load_cwl(cwl_test_file_path)
        self.assertIsInstance(graph, Graph)
        self.assertListEqual(graph.roots, [0])
        self.assertDictEqual(graph.dependencies, {})

    def test_1_reader_step_graph_node_ids(self):
        cwl_test_file_path = Path("../cwl_examples/steps/echo.cwl")
        reader = Reader()
        graph = reader.load_cwl(cwl_test_file_path)
        self.assertListEqual(graph.roots, [0])
        graph = reader.load_cwl(cwl_test_file_path)
        self.assertListEqual(graph.roots, [1])

        reader = Reader(24)
        graph = reader.load_cwl(cwl_test_file_path)
        self.assertListEqual(graph.roots, [24])
        graph = reader.load_cwl(cwl_test_file_path)
        self.assertListEqual(graph.roots, [25])


if __name__ == '__main__':
    unittest.main()
