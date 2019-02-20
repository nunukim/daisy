import unittest as ut
import shutil
import os

from daisy.formatted_target import get_formatted_class
from daisy.formatted_target import CsvTarget, NpyTarget, JsonTarget, PickleTarget

import pandas as pd
import numpy as np

class _DummyClass(object):
    def a(self):
        return 1

    def __init__(self):
        self.b = "b"

class TestTarget(ut.TestCase):
    def setUp(self):
        self.tmp_dir = "./tmp"

        if os.path.exists(self.tmp_dir):
            shutil.rmtree(self.tmp_dir)
        os.mkdir(self.tmp_dir)

    def _save_and_load(self, ext, cls, obj):
        obtained_class = get_formatted_class(ext)
        self.assertEqual(cls, obtained_class)

        targ = cls(self.tmp_dir + "/test." + ext)
        self.assertFalse(targ.exists())

        # Save
        targ.dump(obj)

        self.assertTrue(targ.exists())

        # Load
        obj2 = targ.load()

        return obj2

    def test_csv_target(self):
        obj = pd.DataFrame(np.arange(25).reshape((5,5)), columns=["a","b","c","d","e"], index=[0,1,2,3,4])
        obj2 = self._save_and_load("csv", CsvTarget, obj)

        np.testing.assert_array_equal(obj.values, obj2.values)
        np.testing.assert_array_equal(obj.index, obj2.index)
        np.testing.assert_array_equal(obj.columns, obj2.columns)

    def test_npy_target(self):
        obj = np.arange(25)
        obj2 = self._save_and_load("npy", NpyTarget, obj)

        np.testing.assert_array_equal(obj, obj2)


    def test_json_target(self):
        obj = {
                "obj": {"a": 1},
                "ary": [1,2,3]
           }
        obj2 = self._save_and_load("json", JsonTarget, obj)

        self.assertEqual(obj, obj2)


    def test_pickle_target(self):
        obj = _DummyClass()
        obj2 = self._save_and_load("pkl", PickleTarget, obj)

        self.assertEqual(obj2.a(), 1)
        self.assertEqual(obj2.b, "b")

