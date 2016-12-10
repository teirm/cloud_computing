#!/usr/bin/python

import sys

last_key = None
data_chain = ''

for input_line in sys.stdin:
    input_line = input_line.strip()
    input_array = input_line.split()

    if len(input_array) == 2:

        this_key = input_array[0]
        node_data = input_array[1]

        if last_key == this_key:
            data_chain = '->'.join([data_chain, node_data])
        else:
            if last_key:
                print("{}\t{}".format(last_key, data_chain))
            data_chain = node_data
            last_key = this_key

if last_key == this_key:
    print("{}\t{}".format(last_key, data_chain))
