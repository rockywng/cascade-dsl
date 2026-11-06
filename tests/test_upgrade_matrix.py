"""Upgrade kinds x three independent answers, against real Flink.

For every scenario in upgrade_scenarios.py:
  1. `check-upgrade` (manifests) gives the expected verdict and change codes
  2. the `plan --previous` pin report agrees: it drops state exactly when the restore needs to
  3. a real strict savepoint restore of v2 from v1 succeeds or fails as predicted, and state-loss
     upgrades do restore once loss is allowed
A disagreement between the static checks and Flink is the bug this test exists to catch.
"""
import os
import shutil
import tempfile
import unittest

from cascade_dsl.compiler import compile_pipeline
from cascade_dsl.jvm import TARGET, find_flink_home
from cascade_dsl.plans import restore_test
from cascade_dsl.upgrade import diff_manifests

from upgrade_scenarios import SCENARIOS, copy_example

FLINK = find_flink_home()
READY = FLINK is not None and os.path.exists(os.path.join(TARGET, "cascade-tools.jar"))


class StaticPredictionTest(unittest.TestCase):
    """The logical half runs without Flink."""

    def test_check_upgrade_verdicts(self):
        for sc in SCENARIOS:
            with self.subTest(scenario=sc.name):
                d = tempfile.mkdtemp()
                v1, v2 = copy_example(os.path.join(d, "v1")), copy_example(os.path.join(d, "v2"))
                sc.apply(v2)
                a, b = compile_pipeline(v1), compile_pipeline(v2)
                self.assertTrue(b.ok, [x.format() for x in b.diagnostics.errors])
                verdict, changes = diff_manifests(a.lowered.manifest, b.lowered.manifest)
                self.assertEqual(verdict, sc.verdict, [(c.code, c.message) for c in changes])
                self.assertTrue(set(sc.codes) <= {c.code for c in changes}, [c.code for c in changes])
                shutil.rmtree(d)


@unittest.skipUnless(READY, "set FLINK_HOME and run runtime/build.sh")
class RestoreMatrixTest(unittest.TestCase):
    def test_predictions_match_real_restores(self):
        for sc in SCENARIOS:
            with self.subTest(scenario=sc.name):
                d = tempfile.mkdtemp()
                v1, v2 = copy_example(os.path.join(d, "v1")), copy_example(os.path.join(d, "v2"))
                sc.apply(v2)
                strict = restore_test(v1, v2, FLINK, seconds=4)
                report = strict["report"]
                self.assertEqual(bool(strict.get("restored")), sc.restores, strict.get("error", "")[-600:])
                # the physical prediction: state is dropped exactly when a strict restore fails
                # (max parallelism is checked by Flink, not visible in operator matching)
                if sc.name != "change_max_parallelism":
                    self.assertEqual(report.restorable, sc.restores,
                                     f"pin report predicted restorable={report.restorable}: "
                                     f"dropped {report.dropped_stateful}")
                if not sc.restores and sc.restores_with_loss is not None:
                    lossy = restore_test(v1, v2, FLINK, seconds=4, allow_state_loss=True)
                    self.assertEqual(bool(lossy.get("restored")), sc.restores_with_loss,
                                     lossy.get("error", "")[-600:])
                shutil.rmtree(d)


if __name__ == "__main__":
    unittest.main()
