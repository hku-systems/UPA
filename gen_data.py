import random
import sys
import argparse
import csv
import string

# -9.9993 to -0.010748
#-7.6604e-05 to 0.00011779
#-7.5628 to 0.00010547
#0.00010656 to 5.3849
#-6.9696 to -0.00011706
#7.8542e-05 to -0.00015647
#-0.00026223 to 6.3295
#-4.9134 to 0.00013785
#-0.00012418 to -5.12
#-7.1765e-05 to 0.0001489
#python gen_data.py 5000000 --delimiter "," int -------> 1G

def integer_csv(rows, schema, delimiter):
    generators = []

    generators.append(lambda: random.uniform(-9.9993, -0.010748))
    generators.append(lambda: random.uniform(-7.6604e-05, 0.00011779))
    generators.append(lambda: random.uniform(-7.5628, 0.00010547))
    generators.append(lambda: random.uniform(0.00010656, 5.3849))
    generators.append(lambda: random.uniform(-6.9696,-0.00011706))
    generators.append(lambda: random.uniform(7.8542e-05, -0.00015647))
    generators.append(lambda: random.uniform(-0.00026223, 6.3295))
    generators.append(lambda: random.uniform(-4.9134, 0.00013785))
    generators.append(lambda: random.uniform(-0.00012418, -5.12))
    generators.append(lambda: random.uniform(-7.1765e-05, 0.0001489))
    generators.append(lambda: random.uniform(0, 1))

    ofile = open('../tpch-spark/dataset/ds/ds1.10_gen.csv', "w")
    writer = csv.writer(ofile, delimiter=",")
    for x in range(rows):
        writer.writerow([g() for g in generators])

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Generate a large CSV file.',
        epilog='''"Space is big. You just won't believe how vastly,
        hugely, mind-bogglingly big it is."''')
    parser.add_argument('rows', type=int,
                        help='number of rows to generate')
    parser.add_argument('--delimiter', type=str, default=',', required=False,
                        help='the CSV delimiter')
    parser.add_argument('schema', type=str, nargs='+',
                        choices=['int', 'str', 'float','sec'],
                        help='list of column types to generate')

    args = parser.parse_args()
    integer_csv(args.rows, args.schema, args.delimiter)