"""
Project: tiny-google.py
Module:  performance_analyssi
Authors: Cyrus Ramavarapu and Therese Dachille
Purpose: Performance analysis for comparison
         between MapReduce and Spark
"""
import os
import time

import job_submission


def inverted_index_perf(run_time, job_type, book_count):

    data_line = "{}\t{}\t{}".format(job_type, book_count, run_time)

    with open("inv_ind_perf.dat", "a") as ofp:
        print(data_line, file=ofp)


def search_index_perf(run_time, job_type, word_count):

    data_line = "{}\t{}\t{}".format(job_type, word_count, run_time)

    with open("search_perf.dat", "a") as ofp:
        print(data_line, file=ofp)


def iv_perf_analysis(book_dir):

    replication = 3

    book_count = len(os.listdir(book_dir))

    for _ in range(replication):
        print("------PERF TEST 1: SPARK------")
        start_time = time.time()
        job_submission.submit_spark_index(book_dir)
        run_time = time.time() - start_time
        inverted_index_perf(run_time, "SPARK", book_count)

        print("------PERF TEST 1: MAP_REDUCE------")
        start_time = time.time()
        job_submission.submit_mr_index(book_dir)
        run_time = time.time() - start_time
        inverted_index_perf(run_time, "MAP_REDUCE", book_count)

if __name__ == '__main__':

    BOOK_DIR_LIST = ['../books',
                     '../books_1',
                     '../books_3',
                     '../books_4']

    for BOOK_DIR in BOOK_DIR_LIST:
        iv_perf_analysis(BOOK_DIR)
