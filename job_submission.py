"""
Project: tiny-google.py
Module: Job Submission
Authors: Cyrus Ramavarapu and Therese Dachille
Purpose: Functions for spawing MapReduce and SparkJobs
"""

import subprocess
import shlex
import os.path


def submit_mr_index():
    """
    Will submit the indexing map reduce
    job.

    Keyword arguments:
    doc_path -- path to the document to index
    out_dir -- directory for output

    Returns: Int
    """


def submit_mr_rar():
    """
    Will submit the map reduce rank and
    retrieval job.

    Keyword arguments:
    UNSURE RIGHT NOW

    Returns: Int
    """


def submit_spark_index():
    """
    Will submit the indexing spark job

    Keyword arguments:
    doc_path -- path to the document to index
    out_dir -- directory for output

    Returns: Int
    """
# THE FOLLOWING IS AN EXAMPLE OF USING SUBPROCESSES
    example_dir = 'spark_example'
    spark_script = 'pi_est.scala'

    prog_path = os.path.join(example_dir, spark_script)
    part_count = 100000

    args_dict = {'job_name': prog_path,
                 'n_val': part_count
                }

    cmd_string = """spark-shell -i
                    {job_name}
                    --conf spark.driver.extraJavaOptions="-D{n_val}"
                    """.format(**args_dict)
    
    print(shlex.split(cmd_string))
    subprocess.run(shlex.split(cmd_string))


def submit_spark_rar():
    """
    Will submit the spark rank and retrieval job.

    Keyword arguments:
    UNSURE RIGHT NOW

    Returns: Int
    """

if __name__ == '__main__':

    submit_spark_index()
