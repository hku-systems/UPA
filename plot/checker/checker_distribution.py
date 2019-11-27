import random
import numpy as np
import sys

#python checker_distribution.py 2 1: 2 differing records

num_p = int(sys.argv[1])
dist = int(sys.argv[2])
slow_path = 0
num_neighbour = int(sys.argv[3]) #total number of neighbouring records
num_data = 1600000000 #total number of data
interval = 1600000000 // num_p
for j in range(0,100): #700 rounds experiments
    p = np.zeros(num_p)
    for i in range(0,num_neighbour):
        new_index = random.randint(0,num_data) // interval #the random number indicate
        p[new_index] = p[new_index] + 1

    print(p)
    redun_count = [ 1 if p[new_index] == 0  else 0 for i in range(0,num_p)]
    redun_count_sum = sum(redun_count)
    # print(redun_count_sum)
    if redun_count_sum < dist + 1:
        slow_path = slow_path + 1
# print(str(dist + 1))
print(slow_path)



