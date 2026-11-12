"""cascade lineage / impact: pipelines across projects."""
import os
import shutil
import tempfile
import unittest

from cascade_dsl.lineage import impact, mermaid

from test_imports import DOWN, UP


class LineageTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="cascade-lineage-")
        self.write("shop", UP)
        self.write("marketing", DOWN.replace("SRC", "ltv: {from_pipeline: ../shop, sink: ltv_out}")
                   .replace("SELECT customer_id FROM ltv WHERE total > 1000", "SELECT customer_id, total FROM ltv"))

    def tearDown(self):
        shutil.rmtree(self.root)

    def write(self, name, text):
        os.makedirs(os.path.join(self.root, name), exist_ok=True)
        with open(os.path.join(self.root, name, "pipeline.yaml"), "w") as f:
            f.write(text)

    def test_graph(self):
        g = mermaid(self.root)
        self.assertIn('["shop"]', g)
        self.assertIn('|"ltv_out → ltv"|', g)

    def test_impact_of_a_breaking_change(self):
        self.assertEqual([(n, e) for _, n, e in impact(os.path.join(self.root, "shop"), self.root)],
                         [("marketing", [])])
        # the upstream renames the column the downstream model reads
        self.write("shop", UP.replace("SUM(amount) AS total", "SUM(amount) AS lifetime_value"))
        (_, name, errors), = impact(os.path.join(self.root, "shop"), self.root)
        self.assertEqual(name, "marketing")
        self.assertTrue(errors and "total" in errors[0], errors)


if __name__ == "__main__":
    unittest.main()
