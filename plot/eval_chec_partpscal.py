import matplotlib
# matplotlib.use('agg')
import matplotlib.pyplot as plt
import pandas as pd
import os
q = {
    "TPCH1":[2.354,2.621,3.688,9.154],
    "TPCH4":[1.613,1.573,1.593,1.673],
    "TPCH6":[2.332,2.199,2.21,2.26],
    "TPCH11":[1.30,1.531,1.608,1.613],
    "TPCH13":[2.097,2.2,2.277,2.123],
    "TPCH16":[1.381,1.563,1.654,1.654],
    "TPCH21":[1.334,1.402,1.372,1.334]
}
x_label = ['$10^2$','$10^3$','$10^4$','$10^5$']
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
plt.xlabel("Partition size")
plt.legend(loc='upper left')
# plt.show()
plt.savefig(os.path.basename(__file__).replace(".py", ".pdf"))