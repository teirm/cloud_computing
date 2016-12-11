"""
Project: tiny-google.py
Module: Job Submission
Authors: Cyrus Ramavarapu and Therese Dachille
Purpose: Functions for spawing MapReduce and SparkJobs
"""

import subprocess
import shlex
#import os.path


def submit_mr_index():
    """
    Will submit the indexing map reduce
    job.

    Keyword arguments:
    doc_path -- path to the document to index
    out_dir -- directory for output

    Returns: Int
    """

    # make run1:
    # runs count mapper and reducer
    cnt_cmd_string = '''hadoop jar map_reduce_work/hadoop_streaming/hadoop-streaming-2.7.3.jar'''\
        ''' -mapper "python ./count_mapper.py" -reducer "python ./count_reducer.py"'''\
        ''' -input "../input_dir/" -output "./output"'''

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
        ''' -mapper "python ./search_mapper.py {keywords}" -reducer "python ./search_reducer.py"'''\
        ''' -input "./output_2" -output "./search_results"'''.format(keywords)
    subprocess.run(shlex.split(srch_cmd_string))

    # make top_n:
    top_cmd_string = '''hadoop jar map_reduce_work/hadoop_streaming/hadoop-streaming-2.7.3.jar'''\
        ''' -D mapreduce.job.reduces=1 -mapper "python ./top_n_mapper.py {n1}"'''\
        ''' -reducer "python ./top_n_reducer.py {n2}" -input "./search_results"'''\
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
# # THE FOLLOWING IS AN EXAMPLE OF USING SUBPROCESSES
#     working_dir = 'spark_work/scala_funtimes'
# # EXAMINE THE SPARK SCRIPT TO SEE ARGS
#     spark_script = 'adv_wc.scala'

#     prog_path = os.path.join(example_dir, spark_script)

    # args_dict = {'job_name': prog_path
    #             }

    # cmd_string = """spark-shell -i
    #                 {job_name}
    #                 --conf spark.driver.extraJavaOptions="-D{n_val}"
    #                 """.format(**args_dict)

    cmd_string = '''spark-shell -i spark_work/scala_funtimes/adv_wc.scala'''

    subprocess.run(shlex.split(cmd_string))


def submit_spark_rar(keywords, num_results):
    """
    Will submit the spark rank and retrieval job.

    Keyword arguments:
    UNSURE RIGHT NOW

    Returns: Int
    """

    print('Spark_rar method entered with keywords: {key}. It will return the top {n} results'.format(keywords,num_results))

if __name__ == '__main__':
    submit_spark_index()
