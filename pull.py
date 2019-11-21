import subprocess
from pathlib import Path
import sys

cmd1 = "scp -P 2212 john@127.0.0.1:/home/john/AutoDP/eventlog/" + sys.argv[1] + " ./eventlog/"
process = subprocess.Popen(cmd1.split())
output, error = process.communicate()

#app-20191118135134-0005: DP
#app-20191118203110-0006: Original