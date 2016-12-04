# tiny-google.py
# Cyrus Ramavarapu and Therese Dachille
# This program will allow the user to offer text documents to be indexed with a Map Reduce job.
# This program will also allow the user to input keyword(s) to search for in the documents,
# first having Map Reduce rank the documents, then having Python find the exact positions
# of the keyword(s) in the retrieved document(s).

from sys import version_info

py3 = version_info[0] > 2

def main ():
    print('Welcome to tiny-google.\n')

    if py3: 
        choice = int(input('Would you like to:\n1) Input a document to index?\n2) Input keyword(s) to search for?\n'))
    else: 
        choice = int(raw_input('Would you like to:\n1) Input a document to index?\n2) Input keyword(s) to search for?\n'))
    if choice == 1:
        if py3:
            doc_name = input("You have selected to index a document. What is the name of the document? ")
        else:
            doc_name = raw_input("You have selected to index a document. What is the name of the document? ")
        
    elif choice == 2:
        if py3:
            keyword = input("You have selected search for keyword(s). Please enter the keyword(s): ")
        else:
            keyword = raw_input("You have selected search for keyword(s). Please enter the keyword(s): ")
        

if __name__=='__main__':
    main()