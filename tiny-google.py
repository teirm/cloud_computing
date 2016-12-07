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
from sys import version_info 

PY_3 = version_info[0] > 2

def main():
    print('Welcome to tiny-google.\n')

    while (True):
        if PY_3:
            choice = int(input(
                'Would you like to:\n\
                1) Index documents?\n\
                2) Input keyword(s) to search for?\n\
                3) Exit\n'))
                
        else:
            choice = int(raw_input(
                'Would you like to:\n\
                1) Index documents?\n\
                2) Input keyword(s) to search for?\n\
                3) Exit\n'))

        if choice == 1:
            print("You have selected to index a document.\n")
        
            if PY_3:
                app_choice = int(input(
                    'Would you like to:\n\
                    1) MapReduce Indexing\n\
                    2) Spark Indexing\n'))
                    
            else:
                app_choice = int(raw_input(
                    'Would you like to perform indexing using:\n\
                    1) MapReduce\n\
                    2) Spark\n'))
            if app_choice == 1:
                print('You have chosen to index with MapReduce.\nBeginning MapReduce job...\n')
            elif app_choice == 2:
                print('You have chosen to index with Spark.\nBeginning Spark job...\n')

        elif choice == 2:            
            if PY_3:
                keyword = input(
                    "You have selected search for keyword(s).\n\
    Please enter the keyword(s): ")
            else:
                keyword = raw_input(
                    "You have selected search for keyword(s).\n\
    Please enter the keyword(s): ")
            print('Searching for keywords: ', keyword)
        elif choice == 3:
           print('Good-bye!')
           exit(0)
        
if __name__ == '__main__':
    
    main()
