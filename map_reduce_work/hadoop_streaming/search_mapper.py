#!/usr/bin/python

import sys

SEARCH_TERM = sys.argv[1]

for line in sys.stdin:

    line = line.strip()

    (word, data_chain) = line.split('\t')

    if word == SEARCH_TERM:
        data_nodes = data_chain.split('->')

        for data_node in data_nodes:
            (key, value) = data_node.split(':')

            print('{}\t{}'.format(key, value))
