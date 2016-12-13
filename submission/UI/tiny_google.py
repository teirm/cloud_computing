"""
Project: tiny-google.py
Authors: Cyrus Ramavarapu and Therese Dachille
Purpose: This program will allow the user to offer text
         documents to be indexed with a Map Reduce job.
         This program will also allow the user to input
         keyword(s) to search for in the documents,
         first having Map Reduce rank the documents,
         then having Python find the exact positions
         of the keyword(s) in the retrieved document(s).
"""
from sys import stdin
import os
import subprocess
import shlex
import shutil

import job_submission


def main():
    """ main menu for indexing and searching """
    print('Welcome to tiny-google.\n')

    while True:
        print('Would you like to:\n\
            1) Index documents?\n\
            2) Input keyword(s) to search for?\n\
            3) Exit\n')
        choice = int(stdin.readline().rstrip('\n'))

        if choice == 1:
            print("You have selected to index documents.\n")

            print('Would you like to perform indexing using:\n\
                    1) MapReduce\n\
                    2) Spark\n')

            app_choice = int(stdin.readline().rstrip('\n'))

            if app_choice == 1:
                print(
                    'You have chosen to index with MapReduce.\nBeginning MapReduce job...\n')
                job_submission.submit_mr_index()

            elif app_choice == 2:
                print('You have chosen to index with Spark.\nBeginning Spark job...\n')
                job_submission.submit_spark_index()

        elif choice == 2:
            print('You have selected search for keyword(s).\n' \
                    'Please enter the keyword(s) you would like to search for,'\
                        'each separated by a space: ')
            keywords = stdin.readline().rstrip('\n')

            print('\nHow many results would you like to return? ')
            num_results = int(stdin.readline().rstrip('\n'))

            print('And would you like to perform search using:\n\
                    1) MapReduce\n\
                    2) Spark\n')

            app_choice2 = int(stdin.readline().rstrip('\n'))
            app = ""
            if app_choice2 == 1:
                app = "MapReduce"
            elif app_choice2 == 2:
                app = "Spark"

            keys_list = keywords.split(" ")

            keys_str = ''
            
            # format to be 'word1', 'word2', and 'word3'
            for x in range(0, len(keys_list)):
                if x == len(keys_list)-1:
                    keys_str += "and '" + keys_list[x] + "'"
                else:
                    keys_str += "'" + keys_list[x] + "', "


            str_to_prnt = 'You have chosen to search for the top {0} documents'\
                ' for {1} with {2}.\nBeginning {2} job...\n'.format(
                    num_results, keys_str, app)

            print(str_to_prnt)

            if app_choice2 == 1:


                if 'search_results' in os.listdir() and 'top_n_results' in os.listdir():
                    shutil.rmtree('./search_results')
                    shutil.rmtree('./top_n_results')

                job_submission.submit_mr_rar(keywords, num_results)
                # make view:
                subprocess.run(shlex.split('cat ./top_n_results/part-00000'))
                 # read file
                with open('./top_n_results/part-00000', 'r') as f:
                    read_data = f.readlines()

                for line in read_data:
                    line = line.strip()
                    word = line.split('\t')[0]
                    results = line.split('\t')[1]
                    tab_results = results.split('->')
                    print(word, '\n')
                    
                    for res in tab_results:
                        res = res[1:-1]
                        book = line.split(':')[0]
                        freq = line.split(':')[1]
                        print(book, '\t', freq, '\n')

            elif app_choice2 == 2:
                job_submission.submit_spark_rar(keywords, num_results)

                # read file
                with open('./search_results/part-00000', 'r') as f:
                    read_data = f.readlines()
                f.closed

            for line in read_data:
                line = line.strip()
                word = line.split(':')[0]
                results = line.split(':')[1]
                print(word, '\n')
                each_result = results.split(')')
                for res in each_result:
                    res = res[1:]
                    comma_del = res.split(",")
                    if len(comma_del) == 2: 
                        book = comma_del[0]
                        freq = comma_del[1]
                        print(book,'\t',freq,'\n')





        elif choice == 3:
             # make clean:
            # subprocess.run(shlex.split('rm -r ./output/'))
            # subprocess.run(shlex.split('rm -r ./output_2/'))
            # subprocess.run(shlex.split('rm -r ./search_results/'))
            # subprocess.run(shlex.split('rm -r ./top_n_results/'))
            # subprocess.run(shlex.split('rm -r ./inverted_index/'))
            # subprocess.run(shlex.split('rm -r ./search_results/'))
            print('Good-bye!')
            exit(0)

if __name__ == '__main__':

    main()
