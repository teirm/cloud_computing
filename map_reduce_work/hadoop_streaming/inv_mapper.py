#!/usr/bin/python

import sys

for line in sys.stdin:
    line = line.strip()
    (key, count) = line.split('\t')
    (file_name, word) = key.split(' ')

    inv_index_node = ':'.join([file_name, count]) 

    print("{}\t{}".format(word, inv_index_node))
