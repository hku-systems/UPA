import random
import numpy as np
import sys

#python checker_distribution.py 2 1: 2 differing records

def partitioning(num_p,dist,num_neighbour,num_data,interval,depth):
    fast_path = 0
    p = np.zeros(num_p)
    return_depth = [depth]
    # num_neighbour = random.randint(9,100)
    print(num_neighbour)
    for i in range(0,num_neighbour):
        new_index = random.randint(0,num_data) // interval #the random number indicate
        p[new_index] = p[new_index] + 1

    redun_count = [ 1 if int(p[i]) > 0  else 0 for i in range(0,num_p)]
    redun_count_sum = sum(redun_count)
    if redun_count_sum > dist: #fast path
        fast_path = fast_path + 1
    else:
        np_p = np.array(p)
        depth = depth + 1
        more_count = np.extract(np_p >= 2, np_p)
        return_depth_list = []
        for j in range(0,len(more_count)):
            return_depth_list.append(partitioning(num_p,1,int(more_count[j]),interval,interval // num_p,depth))
        return_depth.append(max(return_depth_list))
# print(str(dist + 1))
    return max(return_depth)


num_p = int(sys.argv[1])
dist = int(sys.argv[2])
num_neighbour = int(sys.argv[3]) #total number of neighbouring records
num_data = 1600000000 #total number of data
interval = 1600000000 // num_p
all_depth = []
for j in range(0,700): #700 rounds experiments
    all_depth.append(partitioning(num_p,dist,num_neighbour,num_data,interval,0))
print(all_depth)


