import subprocess
import sys
import random
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import os
from scipy.stats import norm
samp_by_rate = []
data_scal =[1000,10000,100000,1000000]

if sys.argv[1] == "lr":
    data_scal = [1000]

choices_t = [0] + [np.random.normal(0, 1.2345e9, 1)[0] for i in range(15)]
choices = np.sort(np.array(choices_t))
pp = [random.uniform(0.0,10.0) for i in range(15)]
pp_sum = sum(pp)
prob = [0.4] + [p/pp_sum*0.6 for p in pp]

divider = 7.34893e5
for sr in data_scal:
# for sr in [1000]:
    samp = []
    output = 0.0
    num_sample = 0
    with open("/home/john/AutoDP/txts/accuracy/output" + \
              sys.argv[1] + "-10000," + str(sr) + ".txt","r") as ins:
        for line in ins:
            if "samp_output" in line:
                if sys.argv[1] == "11":
                    answer = float(line.split(" ")[1]) // divider
                    if answer < 10000:
                        samp.append(answer + choices[np.random.choice(choices.size,1,prob)[0]]// divider)
                else:
                    samp.append(float(line.split(" ")[1]))
                num_sample = num_sample + 1
            if "final output" in line:
                if sys.argv[1] == "11":
                    output = float(line.split(" ")[2]) // divider
                else:
                    output = float(line.split(" ")[2])
        c_samp = [output for j in range(sr - num_sample)]
        print(sr - num_sample)
        samp_by_rate.append(samp + c_samp)
if sys.argv[1] == "lr":
    tmp_samp = []
    for i in [1000,10000,100000]:
        tmp_samp.append(np.random.choice(samp_by_rate[0],i).tolist())
    tmp_samp.append(samp_by_rate[0])
    samp_by_rate = tmp_samp

colours = ['tab:red','tab:blue','tab:green','tab:orange', \
           'tab:purple','tab:brown','tab:pink','tab:gray', \
           'tab:olive','tab:cyan']
labels = ['$10^3$','$10^4$','$10^5$','Ground truth','$10^7$']
fig, ax = plt.subplots()
target_samp = np.array(samp_by_rate[0])
if sys.argv[1] == '1':
    target_samp = np.random.choice(samp_by_rate[1],1000)

all_samp = np.concatenate((np.array(samp_by_rate[3]),np.array(samp_by_rate[2]),np.array(samp_by_rate[1]),target_samp))


ax.hist(all_samp, bins=100,color = colours[3],label = labels[3],alpha=0.2)
for j in [2,1]:
    ax.hist(samp_by_rate[j], bins=100,color = colours[j],label = labels[j],alpha=0.4)
ax.hist(target_samp, bins=100,color = colours[0], label = '$10^3$',alpha=0.4)

#=========normal=============
mu_all = []
std_all = []
mu, std = norm.fit(target_samp)
mu_all.append(mu)
std_all.append(std)
for i in [1,2]:
    mu, std = norm.fit(samp_by_rate[j])
    mu_all.append(mu)
    std_all.append(std)
mu, std = norm.fit(all_samp)
mu_all.append(mu)
std_all.append(std)

max_bound = max(all_samp)
min_bound = min(all_samp)
dash_sapce = [4,2,1]
alpha_space = [1.0,0.5,0.5]
for i in [2,1,0]:
    sd = std_all[i]
    if std_all[i] == 0:
        sd = 1
    x = np.linspace(mu_all[i] - 4*sd, mu_all[i] + 4*sd, 100)
    p = norm.pdf(x, mu_all[i], sd)
    samp_max = max(p)
    samp_max = min(p)
    ax.plot(x, p,linestyle='--', dashes=(5, dash_sapce[i]), linewidth=2, color = colours[i],label = labels[i], alpha = alpha_space[i])
#=========normal=============

print("max_bound: " + str(max_bound))
print("min_bound: " + str(min_bound))
print("samp_max: " + str(mu_all[0] + 4*std_all[0]))
print("samp_min: " + str(mu_all[0] - 4*std_all[0]))

x_max = max(max_bound, mu_all[0] + 4*std_all[0])
x_min = min(min_bound, mu_all[0] - 4*std_all[0])

ax.set_ylabel("Frequency")
ax.set_xlabel("Output values")
# ax.set_xticks(np.linspace(x_min,x_max,3),np.linspace(x_min,x_max,3))
y_formatter = matplotlib.ticker.ScalarFormatter(useOffset=True)
ax.xaxis.set_major_formatter(y_formatter)
ax.ticklabel_format(axis='x', style='sci', scilimits=(1,4))
ax.legend()
plt.yscale("log")
plt.savefig(os.path.basename(__file__).replace(".py", ".pdf"))