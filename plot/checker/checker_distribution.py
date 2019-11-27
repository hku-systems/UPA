import random
import numpy as np
import sys

num_p = int(sys.argv[1])
dist = int(sys.argv[2])
p = np.zeros(num_p)
slow_path = 0
num_neighbour = 200 #total number of neighbouring records
num_data = 1600000000 #total number of data
for j in range(0,700): #700 rounds experiments
    for i in range(0,num_neighbour):
        new_index = random.randint(num_data) // num_p #the random number indicate
        p[new_index] = p[new_index] + 1

    redun_count = [ 1 if p[new_index] > 0  else 0 for i in range(0,num_p)]
    redun_count_sum = sum(redun_count)
    if redun_count_sum <= dist + 1:
        slow_path = slow_path + 1
print(slow_path)



