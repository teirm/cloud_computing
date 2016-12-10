#!/usr/bin/python

import sys

MAX_RANK = sys.argv[1]

TOP_VALUES = []

LAST_KEY = None

for line in sys.stdin:
    line = line.strip()
    
    (this_key, data_node) = line.split('\t')
    (book, value) = data_node.split(':')

    if LAST_KEY == this_key:
        if len(TOP_VALUES) < int(MAX_RANK):
            TOP_VALUES.append((book, value))
        else:
            if value >= TOP_VALUES[-1][1]:
                TOP_VALUES[-1] = (book, value)
    else:
        if LAST_KEY:
            for s_book, s_value in TOP_VALUES:
                output_val = ':'.join([s_book, s_value])
                print('{}\t{}'.format(LAST_KEY, output_val))
        TOP_VALUES = []
        TOP_VALUES.append((book, value))
        LAST_KEY = this_key

    TOP_VALUES.sort(key=lambda x: x[1], reverse=True)


#top_val_string = '->'.join('(%s:%s)' % tup for tup in TOP_VALUES)
#print('{}\t{}'.format(LAST_KEY, top_val_string))

for s_book, s_value in TOP_VALUES:
    output_val = ':'.join([s_book, s_value])
    print('{}\t{}'.format(LAST_KEY, output_val))
