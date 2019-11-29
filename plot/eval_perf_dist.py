import matplotlib
# matplotlib.use('agg')
import matplotlib.pyplot as plt
import pandas as pd
import os
q = {
    "TPCH1":[2.214,2.211,2.178,2.218],
    "TPCH4":[1.52,1.446,1.448,1.532],
    "TPCH6":[2.201,2.202,2.21,2.286],
    "TPCH11":[1.432,1.430,1.437,1.481],
    "TPCH13":[2.177,2.140,2.085,2.072],
    "TPCH16":[1.498,1.511,1.529,1.523],
    "TPCH21":[1.264,1.216,1.227,1.255],
}
x_label = ['$10^1$','$10^2$','$10^3$','$10^4$']
# colours = ['tab:blue','tab:orange','tab:green','tab:red','tab:purple']
colours = ['tab:blue','tab:orange','tab:green','tab:red',\
           'tab:purple','tab:brown','tab:pink','tab:gray', \
           'tab:olive','tab:cyan']
markers=['o','v','^','p','3','2','s','p','1']
#markers=["ro-","bv-","c^-","mp-","ys-","g3-","o2-",""]
counter = 0
for key in q:
    plt.plot(range(0,len(x_label)),q[key],\
             marker=markers[counter],label=key,color=colours[counter])
    counter = counter + 1
plt.xticks(range(0,len(x_label)),x_label)
plt.ylabel("Performance Overhead")
plt.xlabel("Distance")
plt.legend(loc='center right')
# plt.show()
plt.savefig(os.path.basename(__file__).replace(".py", ".pdf"))