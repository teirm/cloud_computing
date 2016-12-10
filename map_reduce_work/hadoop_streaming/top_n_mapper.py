#!/usr/bin/python

import sys

MAX_RANK = sys.argv[1]

TOP_VALUES = []

for line in sys.stdin:
    line = line.strip()
    (book, value) = line.split('\t')

    if len(TOP_VALUES) < int(MAX_RANK):
        TOP_VALUES.append((book, value))
    else:
        if value >= TOP_VALUES[-1][1]:
            TOP_VALUES[-1] = (book, value)

    TOP_VALUES.sort(key=lambda x: x[1], reverse=True)

for book, value in TOP_VALUES:
    print('{}\t{}'.format(book, value))
