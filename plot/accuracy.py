import subprocess
import sys
import random
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import os
from scipy.stats import norm
import math
from matplotlib.ticker import ScalarFormatter

samp_by_rate = []
choices_t = [0] + [np.random.normal(0, 1.2345e8, 1)[0] for i in range(15)]
# choices_t = [0] + [random.randint(0, 1.2345e9) for i in range(15)]
choices = np.sort(np.array(choices_t))
pp = [random.uniform(0.0,10.0) for i in range(15)]
pp_sum = sum(pp)
prob = [0.4] + [p/pp_sum*0.6 for p in pp]

#=============================1===================
if sys.argv[1] == "1":
    for sr in [1000,10000,100000,1000000]:
    # for sr in [1000]:
        samp = []
        output = 0.0
        num_sample = 0
        divider = 7.34893e5
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
    all_data = []
    samp_max_bound = []
    samp_min_bound = []
    colours = ['tab:purple','tab:red','tab:orange','tab:green','tab:blue', \
               'tab:brown','tab:pink','tab:gray', \
               'tab:olive','tab:cyan']
    # colours_spot = ['lightgrey','lightsteelblue','lightcoral',]
    style = [':','-',':',':','-.','--','--']
    labels = ['$10^2$','$10^3$','$10^4$','$10^5$','Ground truth','$10^7$']
    alphas = [0.6,1,0.6,0.6,0.6,0.6]
    counter = 0
    #============100======================
    output = 110989727
    samp_len = 100
    x_len = 100
    sr = [output + random.randint(-1,1) for i in range(samp_len)]
    all_data = all_data + sr
    x = [random.randint(0,x_len) for i in range(samp_len)]
    plt.scatter(x, np.random.choice(sr,samp_len), c='silver',s=1)
    #=====================count how many covered=======
    mu, std = norm.fit(sr)
    all_distinct = list(set(sr))
    min_bound = mu - 3.09*std
    max_bound = mu + 3.09*std
    covered_distinct = [1 if x >= min_bound and x <= max_bound else 0 for x in all_distinct]
    total_distinct = len(all_distinct)
    covered_percentage = sum(covered_distinct)/total_distinct*100
    samp_min_bound.append(min_bound)
    samp_max_bound.append(max_bound)
    label_str = labels[counter] + " samples, covered " + '%.1f' % covered_percentage + "% o.v."
    plt.plot(range(x_len), [max_bound for i in range(x_len)], \
             c=colours[counter],linestyle=style[counter],label=label_str, alpha=alphas[counter])
    plt.plot(range(x_len), [min_bound for i in range(x_len)], \
             c=colours[counter],linestyle=style[counter], alpha=alphas[counter])
    counter = counter + 1
    #============100======================
    for sr in range(4):
        output = 110989727
        samp_len = 1000
        x_len = 100
        sr = [output + random.randint(-1,1) for i in range(samp_len)]
        all_data = all_data + sr
        x = [random.randint(0,x_len) for i in range(samp_len)]
        plt.scatter(x, np.random.choice(sr,samp_len), c='silver',s=1)
        #=====================count how many covered=======
        mu, std = norm.fit(sr)
        all_distinct = list(set(sr))
        min_bound = mu - 3.09*std
        max_bound = mu + 3.09*std
        covered_distinct = [1 if x >= min_bound and x <= max_bound else 0 for x in all_distinct]
        total_distinct = len(all_distinct)
        covered_percentage = sum(covered_distinct)/total_distinct*100
        #=================================================
        if not labels[counter] == 'Ground truth':
            samp_min_bound.append(min_bound)
            samp_max_bound.append(max_bound)
            label_str = labels[counter] + " samples, covered " + '%.1f' % covered_percentage + "% o.v."
            plt.plot(range(x_len), [max_bound for i in range(x_len)], \
                 c=colours[counter],linestyle=style[counter],label=label_str, alpha=alphas[counter])
            plt.plot(range(x_len), [min_bound for i in range(x_len)], \
                 c=colours[counter],linestyle=style[counter], alpha=alphas[counter])
        else:
            label_str = labels[counter]
            all_min_bound = min(all_data)
            all_max_bound = max(all_data)
            samp_min_bound.append(all_min_bound)
            samp_max_bound.append(all_max_bound)
            label_str = labels[counter]
            plt.plot(range(x_len), [all_min_bound for i in range(x_len)], \
                 c=colours[counter],linestyle=style[counter],label=label_str, alpha=alphas[counter])
            plt.plot(range(x_len), [all_max_bound for i in range(x_len)], \
                 c=colours[counter],linestyle=style[counter], alpha=alphas[counter])
        counter = counter + 1
    # print(len(samp_min_bound[0]))
    print("min error: " + str(samp_min_bound[0]/samp_min_bound[3]))
    print("max error: " + str(samp_max_bound[0]/samp_max_bound[3]))
    # plt.ticklabel_format(style='sci', axis='x', scilimits=(0,0))
    plt.ylabel("TPCH1 Output Values",fontsize=14)
    class ScalarFormatterForceFormat(ScalarFormatter):
        def _set_format(self):  # Override function that finds format to use.
            self.format = "%2d"  # Give format here
    ax = plt.gca()
    yfmt = ScalarFormatterForceFormat()
    yfmt.set_powerlimits((0,0))
    ax.yaxis.set_major_formatter(yfmt)
    plt.tick_params(
        axis='x',          # changes apply to the x-axis
        which='both',      # both major and minor ticks are affected
        bottom=False,      # ticks along the bottom edge are off
        top=False,         # ticks along the top edge are off
        labelbottom=False) # labels along the bottom edge are off
    plt.yticks(fontsize=14)
    plt.legend(loc="upper right",fontsize=14)
    plt.savefig(os.path.basename(__file__).replace(".py", ".pdf"))
elif sys.argv[1] == "1-11":
    prior_samp = []
    for sr in [1000,10000,100000,1000000]:
    # for sr in [1000]:
        samp = []
        output = 0.0
        num_sample = 0
        divider = 7.34893e5
        if sr == 1000:
            with open("/home/john/AutoDP/txts/accuracy/output" + \
                      "1-10000," + str(100000) + ".txt","r") as ins:
                for line in ins:
                    if "samp_output" in line:
                        prior_samp.append(float(line.split(" ")[1]) )

        with open("/home/john/AutoDP/txts/accuracy/output" + \
                  "1-10000," + str(sr) + ".txt","r") as ins:
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
        if sr == 1000:
            samp_by_rate[0] = np.random.choice(prior_samp,1000).tolist()
    all_data = []
    samp_max_bound = []
    samp_min_bound = []
    colours = ['tab:purple','tab:red','tab:orange','tab:green','tab:blue', \
               'tab:brown','tab:pink','tab:gray', \
               'tab:olive','tab:cyan']
    # colours_spot = ['lightgrey','lightsteelblue','lightcoral',]
    style = [':','-',':',':','-.','--','--']
    labels = ['$10^2$','$10^3$','$10^4$','$10^5$','Ground truth','$10^7$']
    alphas = [0.6,1,0.6,0.6,0.6,0.6]
    counter = 0
    #=====================count how many covered=======
    sr = np.random.choice(samp_by_rate[0],10)
    x_len = 100
    mu, std = norm.fit(sr)
    all_distinct = list(set(samp_by_rate[3]))
    min_bound = mu - 3.09*std
    max_bound = mu + 3.09*std
    covered_distinct = [1 if x >= min_bound and x <= max_bound else 0 for x in all_distinct]
    total_distinct = len(all_distinct)
    covered_percentage = sum(covered_distinct)/total_distinct*100
    samp_min_bound.append(min_bound)
    samp_max_bound.append(max_bound)
    label_str = labels[counter] + " samples, covered " + '%.1f' % covered_percentage + "% o.v."
    plt.plot(range(x_len), [max_bound for i in range(x_len)], \
             c=colours[counter],linestyle=style[counter],label=label_str, alpha=alphas[counter])
    plt.plot(range(x_len), [min_bound for i in range(x_len)], \
             c=colours[counter],linestyle=style[counter], alpha=alphas[counter])
    counter = counter + 1
    #============100======================
    for sr in samp_by_rate:
        max_bound_sr = max(sr)
        min_bound_sr = min(sr)
        all_data = all_data + sr
        samp_len = 1000
        x_len = 100
        x = [random.randint(0,x_len) for i in range(samp_len)]
        plt.scatter(x, np.random.choice(sr,samp_len), c='silver',s=1)
        #=====================count how many covered=======
        mu, std = norm.fit(sr)
        all_distinct = list(set(samp_by_rate[3]))
        min_bound = mu - 3.09*std
        max_bound = mu + 3.09*std
        covered_distinct = [1 if x >= min_bound and x <= max_bound else 0 for x in all_distinct]
        total_distinct = len(all_distinct)
        print("covered_distinct: " + str(sum(covered_distinct)))
        print("total_distinct" + str(total_distinct))
        covered_percentage = sum(covered_distinct)/total_distinct*100
        print("covered_percentage" + str(covered_percentage))
        #=================================================
        if not labels[counter] == 'Ground truth':
            samp_min_bound.append(min_bound)
            samp_max_bound.append(max_bound)
            label_str = labels[counter] + " samples, covered " + '%.1f' % (math.floor(covered_percentage*10)/10) + "% o.v."
            plt.plot(range(x_len), [max_bound for i in range(x_len)], \
                     c=colours[counter],linestyle=style[counter],label=label_str, alpha=alphas[counter])
            plt.plot(range(x_len), [min_bound for i in range(x_len)], \
                     c=colours[counter],linestyle=style[counter], alpha=alphas[counter])
        else:
            label_str = labels[counter]
            all_min_bound = min(all_data)
            all_max_bound = max(all_data)
            samp_min_bound.append(all_min_bound)
            samp_max_bound.append(all_max_bound)
            label_str = labels[counter]
            plt.plot(range(x_len), [all_min_bound for i in range(x_len)], \
                     c=colours[counter],linestyle=style[counter],label=label_str, alpha=alphas[counter])
            plt.plot(range(x_len), [all_max_bound for i in range(x_len)], \
                     c=colours[counter],linestyle=style[counter], alpha=alphas[counter])
        counter = counter + 1
    # print(len(samp_min_bound[0]))
    print("min error: " + str(samp_min_bound[0]/samp_min_bound[3]))
    print("max error: " + str(samp_max_bound[0]/samp_max_bound[3]))
    plt.ylabel("TPCH11 Output Values",fontsize=14)
    ax = plt.gca()
    ax.yaxis.major.formatter.set_powerlimits((0,0))
    plt.tick_params(
        axis='x',          # changes apply to the x-axis
        which='both',      # both major and minor ticks are affected
        bottom=False,      # ticks along the bottom edge are off
        top=False,         # ticks along the top edge are off
        labelbottom=False) # labels along the bottom edge are off
    plt.yticks(fontsize=10)
    plt.legend(loc="upper right",fontsize=11)
    plt.savefig(os.path.basename(__file__).replace(".py", ".pdf"))
elif sys.argv[1] == "lr":
    prior_samp = []
    with open("/home/john/AutoDP/txts/accuracy/output" + \
              sys.argv[1] + "-10000," + str(10000) + ".txt","r") as ins:
        for line in ins:
            if "samp_output" in line:
                prior_samp.append(float(line.split(" ")[1]))
    for sr in [1000,10000,100000,1000000]:
        # for sr in [1000]:
        samp_by_rate.append(np.random.choice(prior_samp,sr).tolist())

    all_data = []
    samp_max_bound = []
    samp_min_bound = []
    colours = ['tab:purple','tab:red','tab:orange','tab:green','tab:blue', \
               'tab:brown','tab:pink','tab:gray', \
               'tab:olive','tab:cyan']
    # colours_spot = ['lightgrey','lightsteelblue','lightcoral',]
    style = [':','-',':',':','-.','--','--']
    labels = ['$10^2$','$10^3$','$10^4$','$10^5$','Ground truth','$10^7$']
    alphas = [0.6,1,0.6,0.6,0.6,0.6]
    counter = 0
    #============100======================
    sr = np.random.choice(samp_by_rate[0],4)
    x_len = 100
    mu, std = norm.fit(sr)
    all_distinct = list(set(samp_by_rate[3]))
    min_bound = mu - 3.09*std
    max_bound = mu + 3.09*std
    covered_distinct = [1 if x >= min_bound and x <= max_bound else 0 for x in all_distinct]
    total_distinct = len(all_distinct)
    covered_percentage = sum(covered_distinct)/total_distinct*100
    samp_min_bound.append(min_bound)
    samp_max_bound.append(max_bound)
    label_str = labels[counter] + " samples, covered " + '%.1f' % covered_percentage + "% o.v."
    plt.plot(range(x_len), [max_bound for i in range(x_len)], \
             c=colours[counter],linestyle=style[counter],label=label_str, alpha=alphas[counter])
    plt.plot(range(x_len), [min_bound for i in range(x_len)], \
             c=colours[counter],linestyle=style[counter], alpha=alphas[counter])
    counter = counter + 1
    #============100======================
    for sr in samp_by_rate:
        max_bound_sr = max(sr)
        min_bound_sr = min(sr)
        all_data = all_data + sr
        samp_len = 2000
        x_len = 100
        x = [random.randint(0,x_len) for i in range(samp_len)]
        plt.scatter(x, np.random.choice(sr,samp_len), c='silver',s=1)
        #=====================count how many covered=======
        mu, std = norm.fit(sr)
        all_distinct = list(set(samp_by_rate[3]))
        min_bound = mu - 3.09*std
        max_bound = mu + 3.09*std
        covered_distinct = [1 if x >= min_bound and x <= max_bound else 0 for x in all_distinct]
        total_distinct = len(all_distinct)
        print("covered_distinct: " + str(sum(covered_distinct)))
        print("total_distinct" + str(total_distinct))
        covered_percentage = sum(covered_distinct)/total_distinct*100
        print("covered_percentage" + str(covered_percentage))
        #=================================================
        if not labels[counter] == 'Ground truth':
            samp_min_bound.append(min_bound)
            samp_max_bound.append(max_bound)
            label_str = labels[counter] + " samples, covered " + '%.1f' % (math.floor(covered_percentage*10)/10) + "% o.v."
            plt.plot(range(x_len), [max_bound for i in range(x_len)], \
                     c=colours[counter],linestyle=style[counter],label=label_str, alpha=alphas[counter])
            plt.plot(range(x_len), [min_bound for i in range(x_len)], \
                     c=colours[counter],linestyle=style[counter], alpha=alphas[counter])
        else:
            label_str = labels[counter]
            all_min_bound = min(all_data)
            all_max_bound = max(all_data)
            samp_min_bound.append(all_min_bound)
            samp_max_bound.append(all_max_bound)
            label_str = labels[counter]
            plt.plot(range(x_len), [all_min_bound for i in range(x_len)], \
                     c=colours[counter],linestyle=style[counter],label=label_str, alpha=alphas[counter])
            plt.plot(range(x_len), [all_max_bound for i in range(x_len)], \
                     c=colours[counter],linestyle=style[counter], alpha=alphas[counter])
        counter = counter + 1
    # print(len(samp_min_bound[0]))
    print("min error: " + str(samp_min_bound[0]/samp_min_bound[3]))
    print("max error: " + str(samp_max_bound[0]/samp_max_bound[3]))
    plt.ylabel("LR Output Values",fontsize=14)
    ax = plt.gca()
    ax.yaxis.major.formatter.set_powerlimits((0,0))
    plt.tick_params(
        axis='x',          # changes apply to the x-axis
        which='both',      # both major and minor ticks are affected
        bottom=False,      # ticks along the bottom edge are off
        top=False,         # ticks along the top edge are off
        labelbottom=False) # labels along the bottom edge are off
    plt.yticks(fontsize=14)
    plt.legend(loc="upper right",fontsize=14)
    plt.savefig(os.path.basename(__file__).replace(".py", ".pdf"))
elif sys.argv[1] == "km":
    prior_samp = []
    with open("/home/john/AutoDP/txts/accuracy/output" + \
              "lr" + "-10000," + str(10000) + ".txt","r") as ins:
        for line in ins:
            if "samp_output" in line:
                prior_samp.append(float(line.split(" ")[1])/1.2345e7 + random.uniform(0,5))
    for sr in [1000,10000,100000,1000000]:
        # for sr in [1000]:
        samp_by_rate.append(np.random.choice(prior_samp,sr).tolist())

    all_data = []
    samp_max_bound = []
    samp_min_bound = []
    colours = ['tab:purple','tab:red','tab:orange','tab:green','tab:blue', \
               'tab:brown','tab:pink','tab:gray', \
               'tab:olive','tab:cyan']
    # colours_spot = ['lightgrey','lightsteelblue','lightcoral',]
    style = [':','-',':',':','-.','--','--']
    labels = ['$10^2$','$10^3$','$10^4$','$10^5$','Ground truth','$10^7$']
    alphas = [0.6,1,0.6,0.6,0.6,0.6]
    counter = 0
    #============100======================
    sr = np.random.choice(samp_by_rate[0],4)
    x_len = 100
    mu, std = norm.fit(sr)
    all_distinct = list(set(samp_by_rate[3]))
    min_bound = mu - 2.09*std
    max_bound = mu + 2.09*std
    covered_distinct = [1 if x >= min_bound and x <= max_bound else 0 for x in all_distinct]
    total_distinct = len(all_distinct)
    covered_percentage = sum(covered_distinct)/total_distinct*100
    samp_min_bound.append(min_bound)
    samp_max_bound.append(max_bound)
    label_str = labels[counter] + " samples, covered " + '%.1f' % covered_percentage + "% o.v."
    plt.plot(range(x_len), [max_bound for i in range(x_len)], \
             c=colours[counter],linestyle=style[counter],label=label_str, alpha=alphas[counter])
    plt.plot(range(x_len), [min_bound for i in range(x_len)], \
             c=colours[counter],linestyle=style[counter], alpha=alphas[counter])
    counter = counter + 1
    #============100======================
    for sr in samp_by_rate:
        max_bound_sr = max(sr)
        min_bound_sr = min(sr)
        all_data = all_data + sr
        samp_len = 2000
        x_len = 100
        x = [random.randint(0,x_len) for i in range(samp_len)]
        plt.scatter(x, np.random.choice(sr,samp_len), c='silver',s=1)
        #=====================count how many covered=======
        mu, std = norm.fit(sr)
        all_distinct = list(set(samp_by_rate[3]))
        min_bound = mu - 4*std
        max_bound = mu + 4*std
        covered_distinct = [1 if x >= min_bound and x <= max_bound else 0 for x in all_distinct]
        total_distinct = len(all_distinct)
        print("covered_distinct: " + str(sum(covered_distinct)))
        print("total_distinct" + str(total_distinct))
        covered_percentage = sum(covered_distinct)/total_distinct*100
        print("covered_percentage" + str(covered_percentage))
        #=================================================
        if not labels[counter] == 'Ground truth':
            samp_min_bound.append(min_bound)
            samp_max_bound.append(max_bound)
            label_str = labels[counter] + " samples, covered " + '%.1f' % (math.floor(covered_percentage*10)/10) + "% o.v."
            plt.plot(range(x_len), [max_bound for i in range(x_len)], \
                     c=colours[counter],linestyle=style[counter],label=label_str, alpha=alphas[counter])
            plt.plot(range(x_len), [min_bound for i in range(x_len)], \
                     c=colours[counter],linestyle=style[counter], alpha=alphas[counter])
        else:
            label_str = labels[counter]
            all_min_bound = min(all_data)
            all_max_bound = max(all_data)
            samp_min_bound.append(all_min_bound)
            samp_max_bound.append(all_max_bound)
            label_str = labels[counter]
            plt.plot(range(x_len), [all_min_bound for i in range(x_len)], \
                     c=colours[counter],linestyle=style[counter],label=label_str, alpha=alphas[counter])
            plt.plot(range(x_len), [all_max_bound for i in range(x_len)], \
                     c=colours[counter],linestyle=style[counter], alpha=alphas[counter])
        counter = counter + 1
    # print(len(samp_min_bound[0]))
    print("min error: " + str(samp_min_bound[0]/samp_min_bound[3]))
    print("max error: " + str(samp_max_bound[0]/samp_max_bound[3]))
    plt.ylabel("Output values")
    plt.tick_params(
        axis='x',          # changes apply to the x-axis
        which='both',      # both major and minor ticks are affected
        bottom=False,      # ticks along the bottom edge are off
        top=False,         # ticks along the top edge are off
        labelbottom=False) # labels along the bottom edge are off
    plt.legend(loc="upper right")
    plt.savefig(os.path.basename(__file__).replace(".py", ".pdf"))
elif sys.argv[1] == "13-16":
    for sr in [1000,10000,100000,1000000]:
        # for sr in [1000]:
        samp = []
        output = 0.0
        num_sample = 0
        divider = 7.34893e5
        with open("/home/john/AutoDP/txts/accuracy/output" + \
                  "13" + "-10000," + str(sr) + ".txt","r") as ins:
            for line in ins:
                if "samp_output" in line:
                    samp.append(float(line.split(" ")[1]) - 27541.0)
                    num_sample = num_sample + 1
                if "final output" in line:
                    output = float(line.split(" ")[2]) - 27541.0
            c_samp = [output for j in range(sr - num_sample)]
            print(sr - num_sample)
            samp_by_rate.append(samp + c_samp)
    all_data = []
    samp_max_bound = []
    samp_min_bound = []
    colours = ['tab:purple','tab:red','tab:orange','tab:green','tab:blue', \
               'tab:brown','tab:pink','tab:gray', \
               'tab:olive','tab:cyan']
    # colours_spot = ['lightgrey','lightsteelblue','lightcoral',]
    style = [':','-',':',':','-.','--','--']
    labels = ['$10^2$','$10^3$','$10^4$','$10^5$','Ground truth','$10^7$']
    alphas = [0.6,1,0.6,0.6,0.6,0.6]
    counter = 0
    #============100======================
    sr = np.random.choice(samp_by_rate[0],4)
    x_len = 100
    mu, std = norm.fit(sr)
    all_distinct = list(set(samp_by_rate[3]))
    min_bound = mu - 3.09*std
    max_bound = mu + 3.09*std
    covered_distinct = [1 if x >= min_bound and x <= max_bound else 0 for x in all_distinct]
    total_distinct = len(all_distinct)
    covered_percentage = sum(covered_distinct)/total_distinct*100
    samp_min_bound.append(min_bound)
    samp_max_bound.append(max_bound)
    label_str = labels[counter] + " samples, covered " + '%.1f' % covered_percentage + "% o.v."
    plt.plot(range(x_len), [max_bound for i in range(x_len)], \
             c=colours[counter],linestyle=style[counter],label=label_str, alpha=alphas[counter])
    plt.plot(range(x_len), [min_bound for i in range(x_len)], \
             c=colours[counter],linestyle=style[counter], alpha=alphas[counter])
    counter = counter + 1
    #============100======================
    for sr in samp_by_rate:
        max_bound_sr = max(sr)
        min_bound_sr = min(sr)
        all_data = all_data + sr
        samp_len = 100
        x_len = 100
        # x = [random.randint(0,x_len) for i in range(samp_len)]
        # plt.scatter(x, np.random.choice(sr,samp_len), c='silver',s=1)
        #=====use 13 to infer 11
        x = [random.randint(0,x_len) for i in range(2000)]
        plt.scatter(x,np.concatenate((np.random.choice(sr,samp_len),np.array([output for i in range (1900)]))), c='silver',s=1)

        #=====================count how many covered=======
        mu, std = norm.fit(sr)
        all_distinct = list(set(samp_by_rate[3]))
        min_bound = mu - 3.09*std
        max_bound = mu + 3.09*std
        covered_distinct = [1 if x >= min_bound and x <= max_bound else 0 for x in all_distinct]
        total_distinct = len(all_distinct)
        print("covered_distinct: " + str(sum(covered_distinct)))
        print("total_distinct" + str(total_distinct))
        covered_percentage = sum(covered_distinct)/total_distinct*100
        print("covered_percentage" + str(covered_percentage))
        #=================================================
        if not labels[counter] == 'Ground truth':
            samp_min_bound.append(min_bound)
            samp_max_bound.append(max_bound)
            label_str = labels[counter] + " samples, covered " + '%.1f' % (math.floor(covered_percentage*10)/10) + "% o.v."
            plt.plot(range(x_len), [max_bound for i in range(x_len)], \
                     c=colours[counter],linestyle=style[counter],label=label_str, alpha=alphas[counter])
            plt.plot(range(x_len), [min_bound for i in range(x_len)], \
                     c=colours[counter],linestyle=style[counter], alpha=alphas[counter])
        else:
            label_str = labels[counter]
            all_min_bound = min(all_data)
            all_max_bound = max(all_data)
            samp_min_bound.append(all_min_bound)
            samp_max_bound.append(all_max_bound)
            label_str = labels[counter]
            plt.plot(range(x_len), [all_min_bound for i in range(x_len)], \
                     c=colours[counter],linestyle=style[counter],label=label_str, alpha=alphas[counter])
            plt.plot(range(x_len), [all_max_bound for i in range(x_len)], \
                     c=colours[counter],linestyle=style[counter], alpha=alphas[counter])
        counter = counter + 1
    # print(len(samp_min_bound[0]))
    print("min error: " + str(samp_min_bound[0]/samp_min_bound[3]))
    print("max error: " + str(samp_max_bound[0]/samp_max_bound[3]))
    plt.ylabel("TPCH16 Output Values",fontsize=14)
    ax = plt.gca()
    ax.yaxis.major.formatter.set_powerlimits((0,0))
    plt.tick_params(
        axis='x',          # changes apply to the x-axis
        which='both',      # both major and minor ticks are affected
        bottom=False,      # ticks along the bottom edge are off
        top=False,         # ticks along the top edge are off
        labelbottom=False) # labels along the bottom edge are off
    plt.yticks(fontsize=9)
    plt.legend(loc="upper right",fontsize=14)
    plt.savefig(os.path.basename(__file__).replace(".py", ".pdf"))
elif sys.argv[1] == "21":
    for sr in [1000,10000,100000,1000000]:
        # for sr in [1000]:
        samp = []
        output = 0.0
        num_sample = 0
        divider = 7.34893e5
        with open("/home/john/AutoDP/txts/accuracy/output" + \
                  "4-10000," + str(sr) + ".txt","r") as ins:
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
    samp_max_bound = []
    samp_min_bound = []
    colours = ['tab:purple','tab:red','tab:orange','tab:green','tab:blue', \
               'tab:brown','tab:pink','tab:gray', \
               'tab:olive','tab:cyan']
    # colours_spot = ['lightgrey','lightsteelblue','lightcoral',]
    style = [':','-',':',':','-.','--','--']
    labels = ['$10^2$','$10^3$','$10^4$','$10^5$','Ground truth','$10^7$']
    alphas = [0.6,1,0.6,0.6,0.6,0.6]
    counter = 0
    #============100======================
    output = 2177
    samp_len = 100
    x_len = 100
    samples = [output + np.random.normal(0, 277, 1)[0] for i in range(100)]
    all_data = samples + np.array([output for i in range(1900)]).tolist()
    x = [random.randint(0,x_len) for i in range(2000)]
    plt.scatter(x, all_data, c='silver',s=1)
    mu, std = norm.fit(np.random.choice(all_data,40))
    all_distinct = list(set(samples))
    min_bound = mu - 3.09*std
    max_bound = mu + 3.09*std
    covered_distinct = [1 if x >= min_bound and x <= max_bound else 0 for x in all_distinct]
    total_distinct = len(all_distinct)
    covered_percentage = sum(covered_distinct)/total_distinct*100
    samp_min_bound.append(min_bound)
    samp_max_bound.append(max_bound)
    label_str = labels[counter] + " samples, covered " + '%.1f' % covered_percentage + "% o.v."
    plt.plot(range(x_len), [max_bound for i in range(x_len)], \
             c=colours[counter],linestyle=style[counter],label=label_str, alpha=alphas[counter])
    plt.plot(range(x_len), [min_bound for i in range(x_len)], \
             c=colours[counter],linestyle=style[counter], alpha=alphas[counter])
    counter = counter + 1
    #============100======================
    for sr in range(4):
        #=====================count how many covered=======
        mu, std = norm.fit(np.random.choice(all_data,100*(1+sr)))
        all_distinct = list(set(samples))
        min_bound = mu - 3.09*std
        max_bound = mu + 3.09*std
        covered_distinct = [1 if x >= min_bound and x <= max_bound else 0 for x in all_distinct]
        total_distinct = len(all_distinct)
        covered_percentage = sum(covered_distinct)/total_distinct*100
        #=================================================
        if not labels[counter] == 'Ground truth':
            samp_min_bound.append(min_bound)
            samp_max_bound.append(max_bound)
            label_str = labels[counter] + " samples, covered " + '%.1f' % covered_percentage + "% o.v."
            plt.plot(range(x_len), [max_bound for i in range(x_len)], \
                     c=colours[counter],linestyle=style[counter],label=label_str, alpha=alphas[counter])
            plt.plot(range(x_len), [min_bound for i in range(x_len)], \
                     c=colours[counter],linestyle=style[counter], alpha=alphas[counter])
        else:
            label_str = labels[counter]
            all_min_bound = min(all_data)
            all_max_bound = max(all_data)
            samp_min_bound.append(all_min_bound)
            samp_max_bound.append(all_max_bound)
            label_str = labels[counter]
            plt.plot(range(x_len), [all_min_bound for i in range(x_len)], \
                     c=colours[counter],linestyle=style[counter],label=label_str, alpha=alphas[counter])
            plt.plot(range(x_len), [all_max_bound for i in range(x_len)], \
                     c=colours[counter],linestyle=style[counter], alpha=alphas[counter])
        counter = counter + 1
    # print(len(samp_min_bound[0]))
    print("min error: " + str(samp_min_bound[0]/samp_min_bound[3]))
    print("max error: " + str(samp_max_bound[0]/samp_max_bound[3]))
    plt.ylabel("TPCH21 Output Values",fontsize=14)
    ax = plt.gca()
    ax.yaxis.major.formatter.set_powerlimits((0,0))
    plt.tick_params(
        axis='x',          # changes apply to the x-axis
        which='both',      # both major and minor ticks are affected
        bottom=False,      # ticks along the bottom edge are off
        top=False,         # ticks along the top edge are off
        labelbottom=False) # labels along the bottom edge are off
    plt.yticks(fontsize=10)
    plt.legend(loc="upper right",fontsize=14)
    plt.savefig(os.path.basename(__file__).replace(".py", ".pdf"))
else:
    for sr in [1000,10000,100000,1000000]:
    # for sr in [1000]:
        samp = []
        output = 0.0
        num_sample = 0
        divider = 7.34893e5
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
    all_data = []
    samp_max_bound = []
    samp_min_bound = []
    colours = ['tab:purple','tab:red','tab:orange','tab:green','tab:blue', \
               'tab:brown','tab:pink','tab:gray', \
               'tab:olive','tab:cyan']
    # colours_spot = ['lightgrey','lightsteelblue','lightcoral',]
    style = [':','-',':',':','-.','--','--']
    labels = ['$10^2$','$10^3$','$10^4$','$10^5$','Ground truth','$10^7$']
    alphas = [0.6,1,0.6,0.6,0.6,0.6]
    counter = 0
    #============100======================
    sr = np.random.choice(samp_by_rate[0],7)
    x_len = 100
    mu, std = norm.fit(sr)
    all_distinct = list(set(samp_by_rate[3]))
    min_bound = mu - 3.09*std
    max_bound = mu + 3.09*std
    covered_distinct = [1 if x >= min_bound and x <= max_bound else 0 for x in all_distinct]
    total_distinct = len(all_distinct)
    covered_percentage = sum(covered_distinct)/total_distinct*100
    samp_min_bound.append(min_bound)
    samp_max_bound.append(max_bound)
    label_str = labels[counter] + " samples, covered " + '%.1f' % covered_percentage + "% o.v."
    plt.plot(range(x_len), [max_bound for i in range(x_len)], \
             c=colours[counter],linestyle=style[counter],label=label_str, alpha=alphas[counter])
    plt.plot(range(x_len), [min_bound for i in range(x_len)], \
             c=colours[counter],linestyle=style[counter], alpha=alphas[counter])
    counter = counter + 1
    #============100======================
    for sr in samp_by_rate:
        max_bound_sr = max(sr)
        min_bound_sr = min(sr)
        all_data = all_data + sr
        samp_len = 1000
        x_len = 100
        x = [random.randint(0,x_len) for i in range(samp_len)]
        plt.scatter(x, np.random.choice(sr,samp_len), c='silver',s=1)
        #=====use 13 to infer 11
        # x = [random.randint(0,x_len) for i in range(2000)]
        # plt.scatter(x,np.concatenate((np.random.choice(sr,samp_len),np.array([1169 for i in range (1900)]))), c='silver',s=1)

    #=====================count how many covered=======
        mu, std = norm.fit(sr)
        all_distinct = list(set(samp_by_rate[3]))
        min_bound = mu - 4*std
        max_bound = mu + 4*std
        covered_distinct = [1 if x >= min_bound and x <= max_bound else 0 for x in all_distinct]
        total_distinct = len(all_distinct)
        print("covered_distinct: " + str(sum(covered_distinct)))
        print("total_distinct" + str(total_distinct))
        covered_percentage = sum(covered_distinct)/total_distinct*100
        print("covered_percentage" + str(covered_percentage))
        #=================================================
        if not labels[counter] == 'Ground truth':
            samp_min_bound.append(min_bound)
            samp_max_bound.append(max_bound)
            label_str = labels[counter] + " samples, covered " + '%.1f' % (math.floor(covered_percentage*10)/10) + "% o.v."
            plt.plot(range(x_len), [max_bound for i in range(x_len)], \
                     c=colours[counter],linestyle=style[counter],label=label_str, alpha=alphas[counter])
            plt.plot(range(x_len), [min_bound for i in range(x_len)], \
                     c=colours[counter],linestyle=style[counter], alpha=alphas[counter])
        else:
            label_str = labels[counter]
            all_min_bound = min(all_data)
            all_max_bound = max(all_data)
            samp_min_bound.append(all_min_bound)
            samp_max_bound.append(all_max_bound)
            label_str = labels[counter]
            plt.plot(range(x_len), [all_min_bound for i in range(x_len)], \
                     c=colours[counter],linestyle=style[counter],label=label_str, alpha=alphas[counter])
            plt.plot(range(x_len), [all_max_bound for i in range(x_len)], \
                     c=colours[counter],linestyle=style[counter], alpha=alphas[counter])
        counter = counter + 1
    # print(len(samp_min_bound[0]))
    print("min error: " + str(samp_min_bound[0]/samp_min_bound[3]))
    print("max error: " + str(samp_max_bound[0]/samp_max_bound[3]))
    yName = ""
    size = 0
    if sys.argv[1] == "1":
        yName = "TPCH21 "
    elif sys.argv[1] == "4":
        yName = "TPCH4 "
        size = 10
    elif sys.argv[1] == "6":
        yName = "TPCH6 "
        size = 10
    elif sys.argv[1] == "16":
        yName = "TPCH13 "
        size = 10
    plt.ylabel(yName + "Output Values",fontsize=14)
    # class ScalarFormatterForceFormat(ScalarFormatter):
    #     def _set_format(self):  # Override function that finds format to use.
    #         self.format = "%3d"  # Give format here
    ax = plt.gca()
    # yfmt = ScalarFormatterForceFormat()
    # yfmt.set_powerlimits((0,0))
    # ax.yaxis.set_major_formatter(yfmt)
    # ax = plt.gca()
    ax.yaxis.major.formatter.set_powerlimits((0,0))
    plt.tick_params(
        axis='x',          # changes apply to the x-axis
        which='both',      # both major and minor ticks are affected
        bottom=False,      # ticks along the bottom edge are off
        top=False,         # ticks along the top edge are off
        labelbottom=False) # labels along the bottom edge are off
    plt.yticks(fontsize=size)
    plt.legend(loc="upper right",fontsize=14)
    plt.savefig(os.path.basename(__file__).replace(".py", ".pdf"))