import subprocess
import sys
import random
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import os
from scipy.stats import norm
samp_by_rate = []
for sr in [1000,10000,100000,1000000]:
# for sr in [1000]:
    samp = []
    output = 0.0
    num_sample = 0
    with open("/home/john/AutoDP/txts/accuracy/output" + \
              sys.argv[1] + "-10000," + str(sr) + ".txt","r") as ins:
        for line in ins:
            if "samp_output" in line:
                samp.append(float(line.split(" ")[1]))
                num_sample = num_sample + 1
            if "final output" in line:
                output = float(line.split(" ")[2])
        c_samp = [output for j in range(sr - num_sample)]
        print(sr - num_sample)
        samp_by_rate.append(samp + c_samp)

colours = ['tab:blue','tab:green','tab:orange','grey', \
           'tab:purple','tab:brown','tab:pink','tab:gray', \
           'tab:olive','tab:cyan']
labels = ['$10^3$','$10^4$','$10^5$','Ground truth','$10^7$']
fig, ax = plt.subplots()
target_samp = np.array(samp_by_rate[0])
if sys.argv[1] == '1':
    target_samp = np.random.choice(samp_by_rate[1],1000)

ax.hist(np.array(samp_by_rate[3]) + np.array(samp_by_rate[2]) + np.array(samp_by_rate[1]) + target_samp, bins='auto',color = colours[3],label = labels[3])
for j in [2,1]:
    ax.hist(samp_by_rate[j], bins='auto',color = colours[j],label = labels[j])

ax.hist(target_samp, bins='auto',color = colours[0], label = '$10^3$',alpha=0.6)

#=========normal=============
mu, std = norm.fit(target_samp)
max_bound = max(samp_by_rate[3])
min_bound = min(samp_by_rate[3])
x = np.linspace(mu - 4*std, mu + 4*std, 1000)
p = norm.pdf(x, mu, std)

samp_max = max(p)
samp_max = min(p)
ax.plot(x, p, '--', linewidth=2, color = "tab:red",label = "UPA's inferred distribution")
#=========normal=============

print("max_bound: " + str(max_bound))
print("min_bound: " + str(min_bound))
print("samp_max: " + str(mu + 4*std))
print("samp_min: " + str(mu - 4*std))

x_max = max(max_bound, mu + 4*std)
x_min = min(min_bound, mu - 4*std)

ax.set_ylabel("Frequency")
ax.set_xlabel("Output values")
# ax.set_xticks(np.linspace(x_min,x_max,3),np.linspace(x_min,x_max,3))
y_formatter = matplotlib.ticker.ScalarFormatter(useOffset=True)
ax.xaxis.set_major_formatter(y_formatter)
ax.ticklabel_format(axis='x', style='sci', scilimits=(1,4))
ax.legend()
plt.yscale("log")
plt.savefig(os.path.basename(__file__).replace(".py", ".pdf"))