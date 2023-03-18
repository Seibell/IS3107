import json
import dateutil
from pymongo import MongoClient

client = MongoClient('localhost', 27017)

## if you have alrd loaded ur dataset, then can just rename the params in
## client[] and mydb[] so that it matches urs
## then no need to load the data again

mydb = client["amazonPhones"]
my_collection = mydb["phonesNaccessories"]



### if you want to clear collection in DB use and uncomment this
# my_collection.delete_many({})


#### --- LOADING DATA INTO DB ---
#### Uncomment this if u want to load data into db

## the file is written to the String contents
#jsonDataSetString = open("Cell_Phones_and_Accessories.json", "r").read()

## load String into a dict, parse by each line
#jsonDataSetDict = [json.loads(str(item)) for item in jsonDataSetString.strip().split('\
#my_collection.insert_many(jsonDataSetDict)


#### ---

# test to see if some data is inserted
exampleTest = my_collection.find_one()
print(type(exampleTest))
print(exampleTest)
type(exampleTest['_id'])

## test output json
# exampleTest['_id'] = 1
# test output
# with open('testOutput.json', 'w') as f:
#     json.dump(exampleTest, f)




import re
from bson import json_util

# Aggregate Queries
## Remove all irrelevant fields
q0 = {"$unset" : ["fit","tech1","tech2","imageURL","imageURLHighRes"]}

## Match all documents that have "Cell Phones" string in category field
q1 = {"$match" :{ "category": { "$in": ["Cell Phones"] } } }

## Match all documents that have some sort of ranking in the general Cell Phones & Accessories
regexQ2 = re.compile("^>#.* in Cell Phones & Accessories \(See Top 100 in Cell Phones & Accessories\)")
q2 = {"$match": { "rank": { "$elemMatch": {"$regex": regexQ2 } } } }

## Add a new field ranking the documents based on the "rank" string. Removes all hashtags and commas.
regexQ3 = re.compile("[#,]")
q3 = {"$addFields": {"overallRankRemovedHash": {"$replaceAll": {"input": {"$arrayElemAt": ["$rank", 0]} , "find": "#", "replacement": ""}}}}

q3p1 = {"$addFields": {"overallRankRemovedRightArrow": {"$replaceAll": {"input": "$overallRankRemovedHash", "find": ">", "replacement" : ""}}}}
q3p2 = {"$addFields": {"overallRankRemovedComma": {"$replaceAll": {"input": "$overallRankRemovedRightArrow", "find": ",", "replacement": ""}}}}

## Now split by whitespace and only select first element (which is the number rank)
q4 = {"$addFields": {"overallRankNumber": {"$toInt": {"$arrayElemAt": [{"$split": ["$overallRankRemovedComma", " "]}, 0] } } } }

## remove the intermediate "rankRemovedHashComma" (and also _id actually)
q5 = {"$unset" : ["overallRankRemovedHash", "overallRankRemovedRightArrow", "overallRankRemovedComma","_id"]}

## sort by rank ascending
sortQ = {"$sort" : {"overallRankNumber" : 1}}

# pipeline
aggPipe = [q0, q1, q2, q3, q3p1, q3p2, q4, q5, sortQ]

# processing thru pipeline
commandCursor = my_collection.aggregate(aggPipe)

# put all results of cursor into dict
documents = []
for document in commandCursor:
    documents.append(document)
    
with open('output.json', 'w') as outfile:
    json.dump(documents, outfile)
    
    
#### Use this if including _id field, because its a bson ObjectId that can't be serialised using the default json dump
# documentsJsonString = json.dumps(documents, default=json_util.default)
# # write list of dicts to file
# with open('output2.json', 'w') as outfile:
#     outfile.write(documentsJsonString)
