import matplotlib
# matplotlib.use('agg')
import matplotlib.pyplot as plt
import pandas as pd
import os
q = {
    "TPCH1":[1.13,1.14,1.20,1.53],
    "TPCH4":[1.08,1.06,1.02,1.06],
    "TPCH6":[1.00,1.20,1.20,1.07],
    "TPCH11":[1.08,1.08,1.08,1.08],
    "TPCH13":[0.97,1.00,1.00,1.10],
    "TPCH16":[1.00,1.00,1.00,1.00],
    "TPCH21":[0.96,0.96,1.03,1.03],
}
x_label = ['1GB','10GB','100GB','1000GB']
# colours = ['tab:blue','tab:orange','tab:green','tab:red','tab:purple']
colours = ['tab:blue','tab:orange','tab:green','tab:red',\
           'tab:purple','tab:brown','tab:pink','tab:gray', \
           'tab:olive','tab:cyan']
markers=['o','v','^','p','3','2','s','p','1']
#markers=["ro-","bv-","c^-","mp-","ys-","g3-","o2-",""]
counter = 0
for key in q:
    plt.plot(range(0,len(x_label)),q[key],marker=markers[counter],label=key,color=colours[counter])
    counter = counter + 1
plt.xticks(range(0,len(x_label)),x_label)
plt.ylabel("Performance Overhead")
plt.xlabel("Sample size")
plt.legend()
plt.show()
# plt.savefig(os.path.basename(__file__).replace(".py", ".pdf"))