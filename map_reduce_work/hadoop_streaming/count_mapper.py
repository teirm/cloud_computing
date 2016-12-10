#!/usr/bin/python

import unicodedata
import string
import sys
import os


for line in sys.stdin:
    line = line.strip()
    words = line.split()

    input_path = os.environ['mapreduce_map_input_file']
    file_name = os.path.split(input_path)[1]

    translator = str.maketrans({key: None for key in string.punctuation})

    for word in words:
        value = 1
        ascii_word = word.encode('ascii', 'ignore').decode('utf-8')
        stripped_word = ascii_word.translate(translator).strip('"')
        key = ' '.join([file_name, stripped_word])
        print("{}\t{}".format(key, value))
