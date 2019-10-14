from __future__ import absolute_import, division, print_function, unicode_literals

import shutil
import os
import subprocess
filelist = os.listdir("/home/senhuang96/silesia/")
dictsizes = [2000, 8000, 16000, 32000, 48000, 64000, 86000, 112640, 140000, 180000]
for filename in filelist:
    for size in dictsizes:
        path = "/home/senhuang96/silesia/" + filename
        cmd = "zstd --train -B1024 " + path + " -o " + path + "dict" + str(size) + " --maxdict " + str(size)
        print(cmd)
        result = subprocess.check_output(cmd, shell=True)
