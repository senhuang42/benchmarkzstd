from __future__ import absolute_import, division, print_function, unicode_literals

import shutil
import os
import subprocess
filelist = os.listdir("/Users/senhuang96/bench/silesia/")
#filesizes = [64000,128000,256000,512000,1000000,2000000,4000000,6000000]
filesizes = [4000, 6000, 8000, 10000, 12000, 14000]

for filename in filelist:
    for size in filesizes:
        path = "/Users/senhuang96/bench/silesia/" + filename
        cmd = "split -a 4 -b " + str(size) + " /Users/senhuang96/bench/silesia/" + filename + " /Users/senhuang96/bench/silesia/" + filename + "_ "
        result = subprocess.check_output(cmd, shell=True)
        cmd = "mkdir " + "/Users/senhuang96/bench/data/" + filename + str(size) + "/"
        result = subprocess.check_output(cmd, shell=True)
        cmd = "mv " + "/Users/senhuang96/bench/silesia/" + filename + "_* " + "/Users/senhuang96/bench/data/" + filename + str(size) + "/"
        print(cmd)
        result = subprocess.check_output(cmd, shell=True)
