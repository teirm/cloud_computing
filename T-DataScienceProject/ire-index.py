import os
import json
import nltk
import string

# read all files that are in subdirectory input/

READ_DATA = ""

# use os.listdir() to read in all files in directory
PATH = "./input/"
DIRS = os.listdir(PATH)

##create data structure for inverted index: dictonary that contains dictionaries
list = {}
list['words'] = {}
list['total#Docs'] = 0

# iterate through files and read them into READ_DATA
for file in DIRS:

    file = PATH+file
    if file != PATH+'.DS_Store':
        list['total#Docs'] += 1
        f = open(file)
        READ_DATA = f.read()
        f.close()
        
        # convert all characters to lower case
        READ_DATA = READ_DATA.lower()
        #print(READ_DATA)

        # eliminate punctuation
        for char in string.punctuation:
            READ_DATA = READ_DATA.replace(char, ' ')
        #print(READ_DATA)

        # eliminate numbers
        for char in string.digits:
            READ_DATA = READ_DATA.replace(char, ' ')
        #print(READ_DATA)

        # perform stemming using nltk stemmer
        tokens = nltk.word_tokenize(READ_DATA)

        porter = nltk.PorterStemmer()
        looper = 0
        for token in tokens:
                tokens[looper] = porter.stem(token)
                looper += 1
        #print "Stemmed -->"
        #print tokens

        for token in tokens:
            #print(token)

            #check if word exists in dictionary
            if list['words'].has_key(token):
                #print(token + ' exists')

                #check if doc already in word's docList
                if list['words'][token]['docList'].has_key(file):
                    list['words'][token]['docList'][file] += 1
                #add file name to docList if not there already
                else:
                    list['words'][token]['docList'][file] = 1
            #add word to dictionary for first time
            else:
                list['words'][token] = {}
                list['words'][token]['docList'] = {}
                list['words'][token]['docList'][file] = 1

                #print(token + " added!")
            
            #update number of docs appears in
            list['words'][token]['wordDocCount'] = len(list['words'][token]['docList'])
            #print(token)
            #print(list['words'][token])

#print(list)

# create .json file with dictionary
f = open('inverted-index.json', 'wb')
json.dump(list,f)
f.close()













