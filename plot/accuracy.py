import subprocess
import sys

samp_by_rate = []
for sr in [1000,10000,100000,1000000]:
    samp = []
    with open("/home/john/AutoDP/txts/accuracy/output" + \
              sys.argv[1] + "-10000," + str(sr) + ".txt","r") as ins:
        for line in ins:
            if "samp_output" in line:
                samp.append(float(line.split(" ")[1]))
        samp_by_rate.append(samp)

max_bound = max(samp_by_rate[0])
min_bound = min(samp_by_rate[0])
for i in range(1,len(samp_by_rate)):
    count_range = [1 if x >= min_bound and x <= max_bound else 0 for x in samp_by_rate[i]]
    count_all = len(samp_by_rate[i])
    print(sum(count_range))
    print(len(samp_by_rate[i]))
    # print(str(float(sum(count_range)/count_all*100)))





