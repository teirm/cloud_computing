#!/usr/bin/python

import sys
import os

for line in sys.stdin:
    line = line.strip()
    keys = line.split()

    input_path = os.environ['mapreduce_map_input_file']
    file_name = os.path.split(input_path)[1]    

    for key in keys:
        value = 1

        key = ' '.join([file_name, key])

        print("{}\t{}".format(key, value))
