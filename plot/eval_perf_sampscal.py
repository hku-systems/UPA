import matplotlib
# matplotlib.use('agg')
import matplotlib.pyplot as plt
import pandas as pd
import os
q = {
    "TPCH1":[1.13444444,1.13222222,1.20333333,1.48777778],
    "TPCH4":[1.06966667,1.046,1.02033333,1.053],
    "TPCH6":[1.00555556,1.14777778,1.16333333,1.06555556],
    "TPCH11":[1.05166667,1.05051282,1.03794872,1.06935897],
    "TPCH13":[0.97606838,0.98376068,0.98589744,1.0974359],
    "TPCH16":[1.01969697,0.96848485,1.01530303,1.01727273],
    "TPCH21":[1.04322377,1.01305718,1.04299865,1.05110311]
}
x_label = ['$10^2$','10^3','10^4','10^5']
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