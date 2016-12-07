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

    if PY_3:
        choice = int(input(
            'Would you like to:\n\
            1) Input a document to index?\n\
            2) Input keyword(s) to search for?\n'))
    else:
        choice = int(raw_input(
            'Would you like to:\n\
            1) Input a document to index?\n\
            2) Input keyword(s) to search for?\n'))

    if choice == 1:
        if PY_3:
            print("You have selected to index a document.\n")
    elif choice == 2:
        if PY_3:
            keyword = input(
                "You have selected search for keyword(s).\n\
Please enter the keyword(s): ")
        else:
            keyword = raw_input(
                "You have selected search for keyword(s).\n\
Please enter the keyword(s): ")


if __name__ == '__main__':
    
    main()
