import matplotlib
# matplotlib.use('agg')
import matplotlib.pyplot as plt
import pandas as pd
import os
q = {
    "TPCH1":[2.268,2.266,2.337,2.621],
    "TPCH4":[1.590,1.566,1.540,1.573],
    "TPCH6":[2.206,2.348,2.363,2.266],
    "TPCH11":[1.436,1.435,1.422,1.454],
    "TPCH13":[2.079,2.086,2.088,2.2],
    "TPCH16":[1.565,1.514,1.561,1.563],
    "TPCH21":[1.259,1.229,1.259,1.267]
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
plt.xlabel("Sample size")
plt.legend(loc='center right')
# plt.show()
plt.savefig(os.path.basename(__file__).replace(".py", ".pdf"))