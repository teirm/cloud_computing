#!/usr/bin/python

import sys

for line in sys.stdin:
    line = line.strip()

    (book, count) = line.split('\t')

    print('{}\t{}'.format(book, count))
