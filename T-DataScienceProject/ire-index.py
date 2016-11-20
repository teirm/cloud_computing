import os
import json
import nltk
import string

# read all files that are in subdirectory input/

read_data = ""

# use os.listdir() to read in all files in directory
path = "./input/"
dirs = os.listdir(path)

##create data structure for inverted index: dictonary that contains dictionaries
list = {}
list['words'] = {}
list['total#Docs'] = 0

# iterate through files and read them into read_data
for file in dirs:

    file = path+file
    if file != path+'.DS_Store':
        list['total#Docs'] += 1
        f = open(file)
        read_data = f.read()
        f.close()
        
        # convert all characters to lower case
        read_data = read_data.lower()
        #print(read_data)

        # eliminate punctuation
        for char in string.punctuation:
            read_data = read_data.replace(char, ' ')
        #print(read_data)

        # eliminate numbers
        for char in string.digits:
            read_data = read_data.replace(char, ' ')
        #print(read_data)

        # perform stemming using nltk stemmer
        tokens = nltk.word_tokenize(read_data)

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













