import subprocess
from pathlib import Path
import sys

cmd1 = "scp -P 2212 john@127.0.0.1:/home/john/AutoDP/eventlog/" + sys.argv[1] + " ./"
process = subprocess.Popen(cmd1.split())
output, error = process.communicate()