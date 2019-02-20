import unittest as ut
import luigi
import numpy as np

from daisy.formatted_target import CsvTarget, PickleTarget
from daisy.task import Task

class TestTask(ut.TestCase):

    def test_output_with_ext(self):

        class _DummyTask(Task):
            ext = "csv"
            param = luigi.Parameter("aaa")

        task = _DummyTask()

        self.assertEqual(task.path(), "./data/_DummyTask/_DummyTask(param=aaa)")

        self.assertIsInstance(task.output(), CsvTarget)
        self.assertEqual(task.output().path,  "./data/_DummyTask/_DummyTask(param=aaa).csv")
        
    def test_output_with_multiple_ext(self):

        class _DummyTask(Task):
            ext = {
                    "users": "csv",
                    "posts": "csv",
                    "large_data": "pkl",
                    }

        task = _DummyTask()

        self.assertEqual(task.path(), "./data/_DummyTask/_DummyTask()")


        out = task.output()

        self.assertEqual(len(list(out.keys())), 3)

        self.assertIsInstance(out["users"], CsvTarget)
        self.assertIsInstance(out["posts"], CsvTarget)
        self.assertIsInstance(out["large_data"], PickleTarget)

        self.assertEqual(out["users"].path, "./data/_DummyTask/_DummyTask()/users.csv")
        self.assertEqual(out["posts"].path,  "./data/_DummyTask/_DummyTask()/posts.csv")
        self.assertEqual(out["large_data"].path,   "./data/_DummyTask/_DummyTask()/large_data.pkl")
        # self.assertIsInstance(out, CsvTarget)
        # self.assertEqual(task.output().path,  "./data/_DummyTask/_DummyTask(param=aaa).csv")


    def test_progress(self):

        class _DummyTask(Task):
            def run(_self):
                ary = [i for i in _self.iter_with_progress(range(1000))]
                np.testing.assert_array_equal(ary, range(1000))


        task = _DummyTask()


        cnt = {"progress": 0, "status": 0}

        # Mocking
        def _mock_progress(perc):
            if cnt["progress"] == 0:
                self.assertEqual(perc, 0.0)
            elif cnt["progress"] == 1:
                self.assertEqual(perc, 100.0)
            cnt["progress"] += 1
        task.set_progress_percentage = _mock_progress

        def _mock_status(msg):
            if cnt["status"] == 0:
                self.assertEqual(msg, "starting: 0 / 1,000 (0.0%)")
            elif cnt["status"] == 1:
                self.assertEqual(msg, "finishing: 1,000 / 1,000 (100.0%)")
            cnt["status"] += 1
        task.set_status_message = _mock_status

        task.run()

        self.assertEqual(cnt["progress"], 2)
        self.assertEqual(cnt["status"], 2)








