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

def iv_perf_analysis(input_dir):

    book_count = len(os.listdir(input_dir))

    print("------PERF TEST 1: SPARK------")
    start_time = time.time()
    job_submission.submit_spark_index()
    run_time = time.time() - start_time

    inverted_index_perf(run_time, "SPARK", book_count)

    print("------PERF TEST 1: MAP_REDUCE------")
    start_time = time.time()
    job_submission.submit_mr_index()
    run_time = time.time() - start_time

    inverted_index_perf(run_time, "MAP_REDUCE", book_count)

if __name__ == '__main__':
   
    book_dir = '../books'  

    iv_perf_analysis(book_dir)





