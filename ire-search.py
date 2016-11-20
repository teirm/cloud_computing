# ire-search.py

# this program reads the JSON object from file inverted-index.json and a set of keywords from file keywords.txt
# and will  produce an ordered list of document filenames, along with their relevance scores and a breakdown of
# weights for all keywords.

import os
import json
import nltk
import math

if not os.path.isfile(os.path.join(os.getcwd(), 'inverted-index.json')):
    print('file not found')

f = open('inverted-index.json', 'rb')
list = json.load(f)
f.close()

file = 'keywords.txt'

f = open(file)
lines = f.readlines()
f.close()

totalDocs = list['total#Docs']

porter = nltk.PorterStemmer()

relDict = {}
relDict['keywords'] = {}

print 'Information Retrieval Engine - Therese Dachille (tcd12@pitt.edu)\n'


# for each set of keywords
for line in lines:

    print('------------------------------------------------------------')

    # creates new list of docs for each set of keywords
    listDocs = []
    # tokenize each line into list of keywords
    line = line.lower()
    #print(line)
    keywords = line.split()
    strKeys = 'keywords ='

    wordDocCount = {}


    # for each keyword in the set, stem the keyword,
    # get the number of docs the word appears in
    # get the docs all keywords in set appears in
    for i in range(0,len(keywords)):
        key = keywords[i]
        strKeys += ' ' + key
        # stem each keyword to find in array
        key = porter.stem(key)
        #keywords[i] = key

        # add each key to dictionary
        relDict['keywords'][key] = {}

        #print(wordDocCount)
        # get number of docs word appears in
        wordDocCount[key] = list['words'][key]['wordDocCount']
        #print(wordDocCount[key])

        # iterate through all docs in each key's list and add to separate list
        for docName in list['words'][key]['docList']:
            docName = docName[8:]
            if docName not in listDocs:           
                listDocs.append(docName)  
            #print(listDocs)

    print(strKeys + '\n')
    #print(listDocs)

    # initialize weightDict for each new set of keywords
    weightDict = {}
    weightDict['keys'] = {}

    # for each keyword in a set
    for key in relDict['keywords']:
        weightDict['keys'][key] = {}
        relDict['keywords'][key]['docs'] = {}
        for doc in listDocs:
            #print(doc)
            if (list['words'][key]['docList'].has_key('./input/' + doc)):
                relDict['keywords'][key]['docs'][doc] = list['words'][key]['docList']['./input/' + doc]
            else:
                relDict['keywords'][key]['docs'][doc] = 0

            # find relevance scores for each keyword
            freq = relDict['keywords'][key]['docs'][doc]
            N = float(totalDocs)
            n = wordDocCount[key]

            if freq is not 0: 
                weight = (1 + math.log(freq,2))*math.log((N/n),2)
            else:
                weight = 0
            weight = format(weight, '.6f')
            weightDict['keys'][key][doc] = weight

    # create dictionary for printing
    finalList = []*len(listDocs)
    scores = []

    # iterate through docs with weights
    for doc in listDocs:
        score = 0
        docDict = {}
        scoreDict = {}
        # add weights together for each keyword in set into variable score
        for key in weightDict['keys']:
            # keyWeight = weight per doc for a given key
            keyWeight = weightDict['keys'][key][doc]
            docDict[key] = keyWeight
            score += float(keyWeight)

            # add to scores list to sort
        scores.append(score)

        for dictionary in docDict:
            scoreDict.update({score: {doc: docDict}})

        finalList.append(scoreDict) 

    # add to finalDict to associate with key, doc, and weights
    scores.sort(reverse=True)
    rank = 1
    # iterate through (sorted) scores and retrieve index from finalList
    for score in scores:
        changeRank = True
        finalListCopy = []
        # for given score, find index in finalList and retrieve dictionary
        for j in range(len(finalList)):
            dictionary = finalList[j]
            # get dictionary from finalList[j] with key as the given score
            if dictionary.has_key(score):
                # retrieve filename, score, weight for each keyword
                for score in dictionary:
                    temp = dictionary.get(score)
                    for doc in temp:
                        s = '''[{0}] file={1}'''.format(rank,doc)
                        print s,
                        fScore = format(score, '.6f')
                        s1 = ''' score={0}'''.format(fScore)
                        print s1
                        # print each keyword for each doc, with weight
                        i = 0
                        for key in temp[doc]: 
                            s2 = '''    weight({0})={1}'''.format(keywords[i],temp[doc][key])
                            print s2
                            i += 1
                        print
                # iterate through finalList and adjust rank accordingly
                for x in range(0, len(finalList)):
                    if x > j:
                        finalListCopy.append(finalList[x])
                break
            else:
                finalListCopy.append(dictionary)

        finalList = finalListCopy
        for f in finalList:
            if f.has_key(score):
                changeRank = False
        if changeRank is True:
            rank += 1
 


