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


def inverted_index_perf(run_no, run_time, job_type, book_count):

    data_line = "{}\t{}\t{}\t{}".format(run_no,
                                        job_type,
                                        book_count,
                                        run_time)

    with open("inv_ind_perf.dat", "a") as ofp:
        print(data_line, file=ofp)


def search_index_perf(run_no, run_time, job_type, word_count, search_size):

    data_line = "{}\t{}\t{}\t{}\t{}".format(run_no,
                                            job_type,
                                            word_count,
                                            search_size,
                                            run_time)

    with open("search_perf.dat", "a") as ofp:
        print(data_line, file=ofp)


def iv_perf_analysis(book_dir):

    replication = 1

    book_count = len(os.listdir(book_dir))

    for run_no in range(replication):
        print("------PERF TEST 1: SPARK------")
        start_time = time.time()
        job_submission.submit_spark_index(book_dir)
        run_time = time.time() - start_time
        inverted_index_perf(run_no, run_time, "SPARK", book_count)

        print("------PERF TEST 1: MAP_REDUCE------")
        start_time = time.time()
        job_submission.submit_mr_index(book_dir)
        run_time = time.time() - start_time
        inverted_index_perf(run_no, run_time, "MAP_REDUCE", book_count)


def search_perf_analysis():

    replication = 1

    max_results = 5

    word_set = ['the',
                'as',
                'is',
                'was',
                'how',
                'but',
                'will',
                'and',
                'then',
                'so']

    for num_words in range(len(word_set)):

        mr_string = " ".join(word_set[:(num_words + 1)])
        spark_string = ",".join(word_set[:(num_words + 1)])

        for result_size in range(max_results):

            for run_no in range(replication):
                print("------PERF TEST 2: SPARK SEARCH------")

                start_time = time.time()
                job_submission.submit_spark_rar(spark_string, result_size)
                run_time = time.time() - start_time
                search_index_perf(run_no,
                                  run_time,
                                  "SPARK",
                                  num_words,
                                  result_size)

                print("------PERF TEST 2: MAP REDUCE SEARCH------")
                start_time = time.time()
                job_submission.submit_mr_rar(mr_string, result_size)
                run_time = time.time() - start_time
                search_index_perf(run_no,
                                  run_time,
                                  "MAP_REDUCE",
                                  num_words,
                                  result_size)


if __name__ == '__main__':

    BOOK_DIR_LIST = ['../books']
    #                 '../books_1',
    #                 '../books_3',
    #                 '../books_4']

    for BOOK_DIR in BOOK_DIR_LIST:
        iv_perf_analysis(BOOK_DIR)

    print('---CLEANING UP BEFORE SEARCHING---')
    job_submission.submit_spark_index()
    job_submission.submit_mr_index()
    print('----CLEAN UP COMPLETE----')

    print('----SEARCH PERFORMANCE TESTING----')
    search_perf_analysis()
