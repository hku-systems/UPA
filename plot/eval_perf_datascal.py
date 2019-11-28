import matplotlib
# matplotlib.use('agg')
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import os
q = {

    "TPCH1":[2.26555556,2.23,2.196,2.15888889,2.12333333,2.08777778],
    "TPCH4":[1.566,1.5081,1.4502,1.3923,1.3344,1.2765],
    "TPCH6":[2.34777778,2.2877,2.22762222,2.16754444,2.10746667,2.04738889],
    "TPCH11":[1.43512821,1.4,1.36487179,1.32974359,1.29461538,1.25948718],
    "TPCH13":[2.08632479,2.08,2.07367521,2.06735043,2.06102564,2.05470085],
    "TPCH16":[1.51393939,1.4623,1.41066061,1.35902121,1.30738182,1.25574242],
    "TPCH21":[1.2287258,1.2058,1.1828742,1.1599484,1.1370226,1.1140968],
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
    plt.plot(range(0,len(x_label)),np.array(q[key]),marker=markers[counter],label=key,color=colours[counter])
    counter = counter + 1
plt.xticks(range(0,len(x_label)),x_label)
plt.ylabel("Performance Overhead")
plt.xlabel("Dataset size")
plt.legend()
# plt.show()
plt.savefig(os.path.basename(__file__).replace(".py", ".pdf"))