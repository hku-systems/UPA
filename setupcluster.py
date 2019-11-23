import subprocess
from pathlib import Path
import sys

op = sys.argv[1]
port = 7081
if op == "up":
    cmd1 = "./sbin/start-master.sh -h 10.22.1.3 -p " + str(port)
    #./sbin/start-master.sh -h 10.22.1.3 -p 7081
    process = subprocess.Popen(cmd1.split())
    output, error = process.communicate()

    cmd1 = "parallel-ssh -h hosts.txt -l john ./AutoDP/sbin/start-slave.sh \
    spark://10.22.1.3:" + str(port)
    #parallel-ssh -h hosts.txt -l john "./AutoDP/sbin/start-slave.sh spark://10.22.1.3:7081"
    process = subprocess.Popen(cmd1.split())
    output, error = process.communicate()

if op == "do":
    cmd1 = "./sbin/stop-master.sh"
    process = subprocess.Popen(cmd1.split())
    output, error = process.communicate()

    cmd1 = "parallel-ssh -h hosts.txt -l john ./AutoDP/sbin/stop-slave.sh \
    spark://10.22.1.3:" + str(port)
    process = subprocess.Popen(cmd1.split())
    output, error = process.communicate()

if op == "upl":
    cmd1 = "./sbin/start-master.sh -h 10.22.1.3 -p " + str(port)
    #./sbin/start-master.sh -h 10.22.1.3 -p 7081
    process = subprocess.Popen(cmd1.split())
    output, error = process.communicate()

    cmd1 = "./sbin/start-slave.sh spark://10.22.1.3:" + str(port)
    #parallel-ssh -h hosts.txt -l john "./AutoDP/sbin/start-slave.sh spark://10.22.1.3:7081"
    process = subprocess.Popen(cmd1.split())
    output, error = process.communicate()

if op == "dol":
    cmd1 = "./sbin/stop-master.sh"
    process = subprocess.Popen(cmd1.split())
    output, error = process.communicate()

    cmd1 = "./sbin/stop-slave.sh spark://10.22.1.3:" + str(port)
    process = subprocess.Popen(cmd1.split())
    output, error = process.communicate()