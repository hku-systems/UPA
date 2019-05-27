## Import the packages
import numpy as np
from scipy import stats
from scipy.stats import ttest_ind, ttest_ind_from_stats
import sys

## Define 2 random distributions
#Sample Size
# N = 10
# #Gaussian distributed data with mean = 2 and var = 1
# a = np.random.randn(N) + 2
# #Gaussian distributed data with with mean = 0 and var = 1
# b = np.random.randn(N)


## Calculate the Standard Deviation
#Calculate the variance to get the standard deviation

#For unbiased max likelihood estimate we have to divide the var by N-1, and therefore the parameter ddof = 1
# var_a = a.var(ddof = 0)*(N)/(N-1)
# var_b = b.var(ddof = 0)*(N)/(N-1)

#std deviation

a_mean = float(sys.argv[1])
b_mean = float(sys.argv[2])
Na = int(sys.argv[5])
Nb = int(sys.argv[6])
var_a = float(sys.argv[3])**2/(Na - 1)
var_b = float(sys.argv[4])**2/(Nb - 1)

s = np.sqrt((var_a/Na + var_b/Nb))

## Calculate the t-statistics
t = (a_mean - b_mean)/(s)

## Compare with the critical t-value
#Degrees of freedom
df = Na + Nb - 2

#p-value after comparison with the t
p = 1 - stats.t.cdf(t,df=df)


print("t = " + str(t))
print("p = " + str(2*p))
### You can see that after comparing the t statistic with the critical t value (computed internally) we get a good p value of 0.0005 and thus we reject the null hypothesis and thus it proves that the mean of the two distributions are different and statistically significant.


## Cross Checking with the internal scipy function
# t2, p2 = stats.ttest_ind(a,b)
# print("t = " + str(t2))
# print("p = " + str(p2))
