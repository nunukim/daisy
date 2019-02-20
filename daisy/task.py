import os
import json

from luigi import *
import luigi
from .formatted_target import get_formatted_class
from .config import daisy as conf

class Task(luigi.Task):
    ext = None

    def task_name(self):
        params = self.to_str_params(only_significant=True, only_public=True)
        param_str = ",".join("{}={}".format(k,v) for k, v in sorted(params.items()))

        return "{}({})".format(self.task_family, param_str.replace("/", "_"))

    def path(self):
        return os.path.join(conf().data_dir, self.get_task_family().replace('.', '/'), self.task_name())

    def output(self):
        if isinstance(self.ext, str):
            cls = get_formatted_class(self.ext)
            return cls(self.path() + "." + self.ext)

        elif isinstance(self.ext, dict):
            path = self.path()
            obj = {}
            for name, ext in self.ext.items():
               cls = get_formatted_class(ext)
               obj[name] = cls(os.path.join(path, name + "." + ext))
            return obj

        else:
            return None




