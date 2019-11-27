import matplotlib
# matplotlib.use('agg')
import matplotlib.pyplot as plt
import pandas as pd
import os
q = {
    "TPCH1":[2.42,2.81666667,1.21333333,1.13222222],
    "TPCH4":[2.21111111,2.26666667,1.245,1.046],
    "TPCH6":[3.75,2.85,1.25531915,1.14777778],
    "TPCH11":[1.92222222,2.01538462,1.00238095,1.05051282],
    "TPCH13":[3,2.54,1.49777778,0.98376068],
    "TPCH16":[3.08888889,2.70833333,1.22435897,0.96848485],
    "TPCH21":[1.94,1.78571429,1.14,1.01305718],
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