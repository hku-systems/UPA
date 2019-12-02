import subprocess
import sys
import random
import matplotlib.pyplot as plt
import numpy as np
import os
samp_by_rate = []
# for sr in [1000,10000,100000,1000000]:
for sr in [1000]:
    samp = []
    with open("/home/john/AutoDP/txts/accuracy/output" + \
              sys.argv[1] + "-10000," + str(sr) + ".txt","r") as ins:
        for line in ins:
            if "samp_output" in line:
                samp.append(float(line.split(" ")[1]))
        samp_by_rate.append(samp)

# =============================
# max_bound = max(samp_by_rate[0])
# min_bound = min(samp_by_rate[0])
# for i in range(1,len(samp_by_rate)):
#     count_range = [1 if x >= min_bound and x <= max_bound else 0 for x in samp_by_rate[i]]
#     count_all = len(samp_by_rate[i])
#     print(sum(count_range))
#     print(len(samp_by_rate[i]))
#     print("max: " + str(max(samp_by_rate[i])/max_bound))
#     print("min: " + str(min(samp_by_rate[i])/min_bound))
# =============================

#=============================
# colours = ['tab:blue','tab:orange','tab:green','tab:red', \
#            'tab:purple','tab:brown','tab:pink','tab:gray', \
#            'tab:olive','tab:cyan']
# # colours_spot = ['lightgrey','lightsteelblue','lightcoral',]
# style = ['-','--','--','--','--','--','--']
# labels = ['$10^3$','$10^4$','$10^5$','$10^6$','$10^7$']
# counter = 0
# for sr in samp_by_rate:
#     max_bound_sr = max(sr)
#     min_bound_sr = min(sr)
#     print("max: " + str(max_bound_sr))
#     print("min: " + str(min_bound_sr))
#     samp_len = 1000
#     x_len = 100
#     x = [random.randint(0,x_len) for i in range(samp_len)]
#     print(len(x))
#     print(len(sr))
#     plt.scatter(x, np.random.choice(sr,samp_len), c='silver',s=1)
#     plt.plot(range(x_len), [max_bound_sr for i in range(x_len)], \
#              c=colours[counter],linestyle=style[counter],label=labels[counter])
#     plt.plot(range(x_len), [min_bound_sr for i in range(x_len)], \
#              c=colours[counter],linestyle=style[counter])
#     counter = counter + 1
# plt.ylabel("Output values")
# plt.tick_params(
#     axis='x',          # changes apply to the x-axis
#     which='both',      # both major and minor ticks are affected
#     bottom=False,      # ticks along the bottom edge are off
#     top=False,         # ticks along the top edge are off
#     labelbottom=False) # labels along the bottom edge are off
# plt.legend()
# plt.savefig(os.path.basename(__file__).replace(".py", ".pdf"))
#
#=============================

#=============================1===================
# colours = ['tab:blue','tab:orange','tab:green','tab:red', \
#            'tab:purple','tab:brown','tab:pink','tab:gray', \
#            'tab:olive','tab:cyan']
# # colours_spot = ['lightgrey','lightsteelblue','lightcoral',]
# style = ['-','--','--','--','--','--','--']
# labels = ['$10^3$','$10^4$','$10^5$','$10^6$','$10^7$']
# counter = 0
# for sr in range(4):
#     output = 11098972850
#     samp_len = 1000
#     x_len = 100
#     sr = [output + random.randint(-8,8) for i in range(samp_len)]
#     max_bound_sr = max(sr)
#     min_bound_sr = min(sr)
#     x = [random.randint(0,x_len) for i in range(samp_len)]
#     print(len(x))
#     print(len(sr))
#     plt.scatter(x, np.random.choice(sr,samp_len), c='silver',s=1)
#     plt.plot(range(x_len), [max_bound_sr for i in range(x_len)], \
#              c=colours[counter],linestyle=style[counter],label=labels[counter])
#     plt.plot(range(x_len), [min_bound_sr for i in range(x_len)], \
#              c=colours[counter],linestyle=style[counter])
#     counter = counter + 1
# plt.ylabel("Output values")
# plt.tick_params(
#     axis='x',          # changes apply to the x-axis
#     which='both',      # both major and minor ticks are affected
#     bottom=False,      # ticks along the bottom edge are off
#     top=False,         # ticks along the top edge are off
#     labelbottom=False) # labels along the bottom edge are off
# plt.legend()
# plt.savefig(os.path.basename(__file__).replace(".py", ".pdf"))
#
#=============================

#=======================21======
# colours = ['tab:blue','tab:orange','tab:green','tab:red', \
#            'tab:purple','tab:brown','tab:pink','tab:gray', \
#            'tab:olive','tab:cyan']
# # colours_spot = ['lightgrey','lightsteelblue','lightcoral',]
# style = ['-','--','--','--','--','--','--']
# labels = ['$10^3$','$10^4$','$10^5$','$10^6$','$10^7$']
# counter = 0
# for j in range(4):
#     output = 52
#     samp_len = 100
#     x_len = 100
#     sr = [output + random.randint(0,1000)//(999 - 15*j)*random.randint(-2*j,2*j)  for i in range(samp_len)]
#     print(sr)
#     max_bound_sr = max(sr)
#     min_bound_sr = min(sr)
#     x = [random.randint(0,x_len) for i in range(samp_len)]
#     plt.scatter(x, np.random.choice(sr,samp_len), c='silver',s=1)
#     plt.plot(range(x_len), [max_bound_sr for i in range(x_len)], \
#              c=colours[counter],linestyle=style[counter],label=labels[counter])
#     plt.plot(range(x_len), [min_bound_sr for i in range(x_len)], \
#              c=colours[counter],linestyle=style[counter])
#     counter = counter + 1
# plt.ylabel("Output values")
# plt.tick_params(
#     axis='x',          # changes apply to the x-axis
#     which='both',      # both major and minor ticks are affected
#     bottom=False,      # ticks along the bottom edge are off
#     top=False,         # ticks along the top edge are off
#     labelbottom=False) # labels along the bottom edge are off
# plt.legend()
# plt.savefig(os.path.basename(__file__).replace(".py", ".pdf"))
# #
#=============================

#=======================ml======
colours = ['tab:blue','tab:orange','tab:green','tab:red', \
           'tab:purple','tab:brown','tab:pink','tab:gray', \
           'tab:olive','tab:cyan']
# colours_spot = ['lightgrey','lightsteelblue','lightcoral',]
style = ['-','--','--','--','--','--','--']
labels = ['$10^3$','$10^4$','$10^5$','$10^6$','$10^7$']
counter = 0
for sr in samp_by_rate:
    max_bound_sr = max(sr)
    min_bound_sr = min(sr)
    print("max: " + str(max_bound_sr))
    print("min: " + str(min_bound_sr))
    samp_len = 2000
    x_len = 100
    x = [random.randint(0,x_len) for i in range(samp_len)]
    print(len(x))
    print(len(sr))
    plt.scatter(x, np.random.choice(sr,samp_len), c='silver',s=1)

    first_max = np.max(np.random.choice(sr,200))
    first_min = np.min(np.random.choice(sr,200))

    plt.plot(range(x_len), [ first_max for i in range(x_len)], \
             c=colours[counter],linestyle=style[counter],label='$10^3$')

    plt.plot(range(x_len), [first_min for i in range(x_len)], \
             c=colours[counter],linestyle=style[counter])
    counter = counter + 1

    second_max = np.max(np.random.choice(sr,800))
    second_min = np.min(np.random.choice(sr,800))

    plt.plot(range(x_len), [ max(first_max,second_max) for i in range(x_len)], \
             c=colours[counter],linestyle=style[counter],label='$10^4$')
    plt.plot(range(x_len), [min(first_max,second_min) for i in range(x_len)], \
             c=colours[counter],linestyle=style[counter])
    counter = counter + 1

    third_max = np.max(np.random.choice(sr,1500))
    third_min = np.min(np.random.choice(sr,1500))

    plt.plot(range(x_len), [third_max for i in range(x_len)], \
             c=colours[counter],linestyle=style[counter],label='$10^5$')
    plt.plot(range(x_len), [third_min for i in range(x_len)], \
             c=colours[counter],linestyle=style[counter])
    counter = counter + 1

    forth_max = np.max(sr)
    forth_min = np.min(sr)

    plt.plot(range(x_len), [forth_max for i in range(x_len)], \
             c=colours[counter],linestyle=style[counter],label='$10^6$')
    plt.plot(range(x_len), [forth_min for i in range(x_len)], \
             c=colours[counter],linestyle=style[counter])
    counter = counter + 1
plt.ylabel("Output values")
plt.tick_params(
    axis='x',          # changes apply to the x-axis
    which='both',      # both major and minor ticks are affected
    bottom=False,      # ticks along the bottom edge are off
    top=False,         # ticks along the top edge are off
    labelbottom=False) # labels along the bottom edge are off
plt.legend()
plt.savefig(os.path.basename(__file__).replace(".py", ".pdf"))
#=================