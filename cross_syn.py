import subprocess
from pathlib import Path
import sys

op = sys.argv[1]

if op == "code":
    cmd1 = "parallel-ssh -h /home/john/AutoDP/hosts.txt -l john -i rm -rf /home/john/AutoDP"
    process = subprocess.Popen(cmd1.split())
    output, error = process.communicate()

    cmd1 = "tar -I pigz -cf autodp.tgz AutoDP"
    process = subprocess.Popen(cmd1.split(),cwd="/home/john/")
    output, error = process.communicate()

    cmd1 = "parallel-scp -h /home/john/AutoDP/hosts.txt -l john /home/john/autodp.tgz /home/john/"
    process = subprocess.Popen(cmd1.split())
    output, error = process.communicate()

    cmd1 = "parallel-ssh -h /home/john/AutoDP/hosts.txt -l john -i pigz -dc autodp.tgz | tar xf -"
    process = subprocess.Popen(cmd1.split())
    output, error = process.communicate()

if op == "data":
    cmd1 = "parallel-ssh -h /home/john/AutoDP/hosts.txt -l john -i rm -rf /home/john/tpch-dbgen"
    process = subprocess.Popen(cmd1.split())
    output, error = process.communicate()

    cmd1 = "tar -I pigz -cf tpch.tgz tpch-dbgen"
    process = subprocess.Popen(cmd1.split(),cwd="/home/john/")
    output, error = process.communicate()

    cmd1 = "parallel-scp -h /home/john/AutoDP/hosts.txt -l john /home/john/tpch.tgz /home/john/"
    process = subprocess.Popen(cmd1.split())
    output, error = process.communicate()

    cmd1 = "parallel-ssh -h /home/john/AutoDP/hosts.txt -l john -t 0 -i pigz -dc tpch.tgz | tar xf -"
    process = subprocess.Popen(cmd1.split())
    output, error = process.communicate()