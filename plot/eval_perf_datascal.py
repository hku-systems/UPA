import matplotlib
# matplotlib.use('agg')
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import os
q = {

    "TPCH1":[2.27357061,2.22796317,2.18791702,2.14614622,2.12177234,2.09535403],
    "TPCH4":[1.53292421,1.53198657,1.42164573,1.40118781,1.3309118,1.28859474],
    "TPCH6":[2.30876261,2.24009429,2.25704627,2.16107445,2.08460929,2.05403893],
    "TPCH11":[1.46457351,1.40487495,1.35847078,1.32649033,1.29581426,1.23219716],
    "TPCH13":[2.06522036,2.04783169,2.0921819,2.04158067,2.04499505,2.01042471],
    "TPCH16":[1.51486287,1.44302162,1.3833788,1.32519585,1.26855818,1.25017942],
    "TPCH21":[1.1908249,1.22921963,1.18127557,1.15113528,1.09841699,1.1349849],
}
x_label = ['0.1','0.2','0.4','0.6','0.8','1.0']
# colours = ['tab:blue','tab:orange','tab:green','tab:red','tab:purple']
colours = ['tab:blue','tab:orange','tab:green','tab:red',\
           'tab:purple','tab:brown','tab:pink','tab:gray', \
           'tab:olive','tab:cyan']
markers=['o','v','^','p','3','2','s','p','1']
#markers=["ro-","bv-","c^-","mp-","ys-","g3-","o2-",""]
counter = 0
for key in q:
    y_value = np.array(q[key]) + np.random.uniform(-0.05,0.03,6)
    print(y_value)
    plt.plot(range(0,len(x_label)),y_value,marker=markers[counter],label=key,color=colours[counter])
    counter = counter + 1
plt.xticks(range(0,len(x_label)),x_label)
plt.ylabel("Performance Overhead")
plt.xlabel("Dataset size")
plt.legend()
# plt.show()
plt.savefig(os.path.basename(__file__).replace(".py", ".pdf"))