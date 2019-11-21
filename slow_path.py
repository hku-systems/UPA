import subprocess
from pathlib import Path
import sys
from random import random
import numpy as np

op = sys.argv[1]

if op == "prob":
    part = np.zeros(20)
    for diff in range(0,10): #number of differing records
        record_count = 1200000000 #1.2B
        samp = random()*record_count
        index = int(samp//60000000)
        print(str(index))
        part[index] = part[index] + 1
    -np.sort(-part)
    print(str(part))



