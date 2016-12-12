"""
Project: tiny-google.py
Module: Job Submission
Authors: Cyrus Ramavarapu and Therese Dachille
Purpose: Functions for spawing MapReduce and SparkJobs
"""

from os import path
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
    cnt_cmd_string = '''hadoop jar ../map_reduce/hadoop-streaming-2.7.3.jar'''\
        ''' -mapper "python ../map_reduce/count_mapper.py" -reducer "python ../map_reduce/count_reducer.py"'''\
        ''' -input "books/" -output "./output"'''

    subprocess.run(shlex.split(cnt_cmd_string))

    # make run2:
    # runs inverted index mapper and reducer
    inv_cmd_string = '''hadoop jar ../map_reduce/hadoop-streaming-2.7.3.jar'''\
        ''' -mapper "python ../map_reduce/inv_mapper.py" -reducer "python ../map_reduce/inv_reducer.py"'''\
        ''' -input "./output" -output "./output_2"'''
    subprocess.run(shlex.split(inv_cmd_string))
    print('\n')


def submit_mr_rar(keywords, num_results):
    """
    Will submit the map reduce rank and
    retrieval job.

    Keyword arguments:
    keyords -- search terms as a space delimited string
    num_results -- the number of results wanted by the user

    Returns: Int
    """

    # make search:
    srch_cmd_string = '''hadoop jar ../map_reduce/hadoop-streaming-2.7.3.jar'''\
        ''' -mapper "python ../map_reduce/search_mapper.py {0}" -reducer "python ../map_reduce/search_reducer.py"'''\
        ''' -input "./output_2" -output "./search_results"'''.format(keywords)
    subprocess.run(shlex.split(srch_cmd_string))

    # make top_n:
    top_cmd_string = '''hadoop jar ../map_reduce/hadoop-streaming-2.7.3.jar'''\
        ''' -D mapreduce.job.reduces=1 -mapper "python ../map_reduce/top_n_mapper.py {0}"'''\
        ''' -reducer "python ../map_reduce/top_n_reducer.py {1}" -input "./search_results"'''\
        ''' -output "./top_n_results"'''.format(num_results, num_results)
    subprocess.run(shlex.split(top_cmd_string))
    print('\n')


## Please leave these two for me.  They require adding some
## code to the spark jobs to handle arguments -- especially for
## the searching -- Cyrus
def submit_spark_index():
    """
    Will submit the indexing spark job

    Keyword arguments:
    doc_path -- path to the document to index
    out_dir -- directory for output

    Returns: Int
    """
    # make clean:
    #!! IN THE FUTURE LOOK INTO OS MODULE TO REMOVE DIRECTORIES
    subprocess.run(shlex.split('rm -r ./inverted_index/'))

    cmd_string = '''spark-shell -i ../spark/create_index.scala'''

    subprocess.run(shlex.split(cmd_string))
    print('\n')


def submit_spark_rar(keywords, num_results):
    """
    Will submit the spark rank and retrieval job.

    Keyword arguments:
    keywords -- search terms as a space delimited string
    num_results -- the number of results wanted by the user

    Returns: Int
    """

    #!!DO NOT DELETE THIS!!#
    # cmd_string = """spark-shell -i {job_name} --conf
    #                spark.driver.extraJavaOptions="-D{n_val}"
    #             """.format(**args_dict)

    # make clean:
    subprocess.run(shlex.split('rm -r ./search_results/'))

    example_dir = '../spark'
    spark_script = 'search_index.scala'
    prog_path = path.join(example_dir, spark_script)

    keyword_arr = keywords.split(" ")
    keyword_list = (",").join(keyword_arr)

    args_dict = {'job_name': prog_path,
                 'keys': keyword_list,
                 'n_val': num_results
                }
    srch_cmd_string = """spark-shell -i
                    {job_name}
                    --conf spark.driver.extraJavaOptions="-D{keys},{n_val}"
                         """.format(**args_dict)

    subprocess.run(shlex.split(srch_cmd_string))
    print('\n')

if __name__ == '__main__':
    submit_spark_index()
