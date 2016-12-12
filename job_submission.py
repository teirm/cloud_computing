"""
Project: tiny-google.py
Module: Job Submission
Authors: Cyrus Ramavarapu and Therese Dachille
Purpose: Functions for spawing MapReduce and SparkJobs
"""

import subprocess
import shlex


def submit_mr_index():
    """
    Will submit the indexing map reduce
    job.

    Keyword arguments:
    doc_path -- path to the document to index
    out_dir -- directory for output

    Returns: Int
    """
    # make clean:
    subprocess.run(shlex.split('rm -r ./output/'))
    subprocess.run(shlex.split('rm -r ./output_2/'))
    subprocess.run(shlex.split('rm -r ./search_results/'))
    subprocess.run(shlex.split('rm -r ./top_n_results/'))
    
    # make run1:
    # runs count mapper and reducer
    cnt_cmd_string = '''hadoop jar map_reduce_work/hadoop_streaming/hadoop-streaming-2.7.3.jar'''\
        ''' -mapper "python ./count_mapper.py" -reducer "python ./count_reducer.py"'''\
        ''' -input "map_reduce_work/input_dir/" -output "./output"'''

    subprocess.run(shlex.split(cnt_cmd_string))

    # make run2:
    # runs inverted index mapper and reducer
    inv_cmd_string = '''hadoop jar map_reduce_work/hadoop_streaming/hadoop-streaming-2.7.3.jar'''\
        ''' -mapper "python ./inv_mapper.py" -reducer "python ./inv_reducer.py"'''\
        ''' -input "./output" -output "./output_2"'''
    subprocess.run(shlex.split(inv_cmd_string))


def submit_mr_rar(keywords, num_results):
    """
    Will submit the map reduce rank and
    retrieval job.

    Keyword arguments:
    UNSURE RIGHT NOW

    Returns: Int
    """

    # make search:
    srch_cmd_string = '''hadoop jar map_reduce_work/hadoop_streaming/hadoop-streaming-2.7.3.jar'''\
        ''' -mapper "python ./search_mapper.py {0}" -reducer "python ./search_reducer.py"'''\
        ''' -input "./output_2" -output "./search_results"'''.format(keywords)
    subprocess.run(shlex.split(srch_cmd_string))

    # make top_n:
    top_cmd_string = '''hadoop jar map_reduce_work/hadoop_streaming/hadoop-streaming-2.7.3.jar'''\
        ''' -D mapreduce.job.reduces=1 -mapper "python ./top_n_mapper.py {0}"'''\
        ''' -reducer "python ./top_n_reducer.py {1}" -input "./search_results"'''\
        ''' -output "./top_n_results"'''.format(num_results, num_results)
    subprocess.run(shlex.split(top_cmd_string))


def submit_spark_index():
    """
    Will submit the indexing spark job

    Keyword arguments:
    doc_path -- path to the document to index
    out_dir -- directory for output

    Returns: Int
    """
    # make clean:
    subprocess.run(shlex.split('rm -r ./inverted_index/'))

    cmd_string = '''spark-shell -i spark_work/scala_funtimes/adv_wc.scala'''

    subprocess.run(shlex.split(cmd_string))


def submit_spark_rar(keywords, num_results):
    """
    Will submit the spark rank and retrieval job.

    Keyword arguments:
    UNSURE RIGHT NOW

    Returns: Int
    """
    # make clean:
    subprocess.run(shlex.split('rm -r ./search_results/'))

    #print('Spark_rar method entered with keywords: {0}. It will return the top {1} results'.format(keywords,num_results))
    srch_cmd_string = '''spark-shell -i spark_work/scala_funtimes/search_index.scala'''

    subprocess.run(shlex.split(srch_cmd_string))

if __name__ == '__main__':
    submit_spark_index()
