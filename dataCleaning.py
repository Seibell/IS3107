#!/usr/bin/env python
# coding: utf-8

# In[3]:


import json
import dateutil
from pymongo import MongoClient
client = MongoClient('localhost', 27017)


# In[5]:


mydb = client["amazonPhones"]


# In[6]:


my_collection = mydb["phonesNaccessories"]


# In[ ]:


# if you want to clear collection in DB use this
# my_collection.delete_many({})


# In[ ]:


# the file is written to the String contents
#jsonDataSetString = open("Cell_Phones_and_Accessories.json", "r").read()

# load String into a dictionary, parse by each line
# i think need to str(item) because item could be loaded as object?? no fking idea, but it works
#jsonDataSetDict = [json.loads(str(item)) for item in jsonDataSetString.strip().split('\n')]


# In[ ]:


# insert json data set
#my_collection.insert_many(jsonDataSetDict)


# In[ ]:


# test to see if some data is inserted
exampleTest = my_collection.find_one()
print(type(exampleTest))
print(exampleTest)


# In[ ]:


import re

# Aggregate Queries
q0 = {"$unset" : ["fit","tech1","tech2","imageURL","imageURLHighRes"]}
q1 = {"$match" :{ "category": { "$in": ["Cell Phones"] } } }
regexQ2 = re.compile("^>#.* in Cell Phones & Accessories \(See Top 100 in Cell Phones & Accessories\)")
q2 = {"$match": { "rank": { "$elemMatch": {"$regex": regexQ2 } } } }

# pipeline
aggPipe = [q0, q1, q2]

# processing thru pipeline
commandCursor = my_collection.aggregate(aggPipe)

# put all results of cursor into dict
documents = []
for document in commandCursor:
    documents.append(document)

print(documents)
# write list of dicts to file
with open('output.json', 'w') as outfile:
    json.dump(documents, outfile)
    

