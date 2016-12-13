#!/usr/bin/python

import sys

SEARCH_TERMS = sys.argv[1:]

for line in sys.stdin:

    line = line.strip()

    (word, data_chain) = line.split('\t')

    if word in SEARCH_TERMS:
        data_nodes = data_chain.split('->')

        for data_node in data_nodes:
            print('{}\t{}'.format(word, data_node))
