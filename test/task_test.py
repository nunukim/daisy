import unittest as ut
import luigi

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




