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
import job_submission


def main():
    """ main menu for indexing and searching """
    print('Welcome to tiny-google.\n')

    while True:
        print('Would you like to:\n\
            1) Index documents?\n\
            2) Input keyword(s) to search for?\n\
            3) Exit\n')
        choice = int(stdin.readline())

        if choice == 1:
            print("You have selected to index documents.\n")

            print('Would you like to perform indexing using:\n\
                    1) MapReduce\n\
                    2) Spark\n')

            app_choice = int(stdin.readline())

            if app_choice == 1:
                print(
                    'You have chosen to index with MapReduce.\nBeginning MapReduce job...\n')
                job_submission.submit_mr_index()

            elif app_choice == 2:
                print('You have chosen to index with Spark.\nBeginning Spark job...\n')
                job_submission.submit_spark_index()

        elif choice == 2:
            print("You have selected search for keyword(s).\n\
    Please enter the keyword(s): ")
            keywords = stdin.readline()

            print('\nHow many results would you like to return? ')
            num_results = int(stdin.readline())

            print('And would you like to perform search using:\n\
                    1) MapReduce\n\
                    2) Spark\n')

            app_choice2 = int(stdin.readline())

            if app_choice2 == 1:
                print(
                    'You have chosen to search for the top {0} documents'
                    'for {1} with MapReduce.'
                    '\nBeginning MapReduce job...\n'.format(
                        num_results, keywords))
                job_submission.submit_mr_rar(keywords, num_results)

            elif app_choice2 == 2:
                print(
                    'You have chosen to search for the top {0} documents for {1} with Spark.'
                    '\nBeginning Spark job...\n'.format(
                        num_results, keywords))
                job_submission.submit_spark_rar(keywords, num_results)

        elif choice == 3:
            print('Good-bye!')
            exit(0)

if __name__ == '__main__':

    main()
