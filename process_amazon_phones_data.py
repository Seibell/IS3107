from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import json
import dateutil
from pymongo import MongoClient
import re
from bson import json_util
import nltk
import nltk.tokenize
from nltk.sentiment.vader import SentimentIntensityAnalyzer

import bson
import parser
from bson.json_util import loads

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'process_amazon_phones_data',
    default_args=default_args,
    description='Process Amazon Phones Data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 16),
    catchup=False,
) as dag:

    def extractAndClean_task():
        client = MongoClient('localhost', 27017)
        mydb = client["Cell_Phones"]
        source_collection = mydb["products"]
        destination_collection = mydb["extractAndClean"]

        # Filtering dataset to remove unwanted elements
        result = mydb.phones.aggregate([
            {
                "$lookup": {
                    "from": "reviews",
                    "localField": "asin",
                    "foreignField": "asin",
                    "as": "reviews"
                }
            },
            {
                "$limit": 1
            }
        ])

        record = result.next()

        print(record)
        # Aggregate Queries
        # Remove all irrelevant fields
        q0 = {"$unset": ["fit", "tech1", "tech2",
                         "imageURL", "imageURLHighRes"]}

    # Match all documents that have "Cell Phones" string in category field
        q1 = {"$match": {"category": {"$in": ["Cell Phones"]}}}

    # Match all documents that have some sort of ranking in the general Cell Phones & Accessories
        regexQ2 = re.compile(
            "^>#.* in Cell Phones & Accessories \(See Top 100 in Cell Phones & Accessories\)")
        q2 = {"$match": {"rank": {"$elemMatch": {"$regex": regexQ2}}}}

    # Add a new field ranking the documents based on the "rank" string. Removes all hashtags and commas.
        regexQ3 = re.compile("[#,]")
        q3 = {"$addFields": {"overallRankRemovedHash": {"$replaceAll": {
            "input": {"$arrayElemAt": ["$rank", 0]}, "find": "#", "replacement": ""}}}}

        q3p1 = {"$addFields": {"overallRankRemovedRightArrow": {"$replaceAll": {
            "input": "$overallRankRemovedHash", "find": ">", "replacement": ""}}}}
        q3p2 = {"$addFields": {"overallRankRemovedComma": {"$replaceAll": {
            "input": "$overallRankRemovedRightArrow", "find": ",", "replacement": ""}}}}

    # Now split by whitespace and only select first element (which is the number rank)
        q4 = {"$addFields": {"overallRankNumber": {"$toInt": {
            "$arrayElemAt": [{"$split": ["$overallRankRemovedComma", " "]}, 0]}}}}

    # remove the intermediate "rankRemovedHashComma" (and also _id actually)
        q5 = {"$unset": ["overallRankRemovedHash",
                         "overallRankRemovedRightArrow", "overallRankRemovedComma", "_id"]}

    # sort by rank ascending
        sortQ = {"$sort": {"overallRankNumber": 1}}

        # joinQ = {"$lookup": {"from": "reviews", "localField": "asin",
        #                      "foreignField": "asin", "as": "result"}}
        aggPipeWithJoin = [q0, q1, q2, q3, q3p1, q3p2, q4, q5, sortQ]
        commandCursorJoined = source_collection.aggregate(aggPipeWithJoin)

        documentsJoined = []
        for document in commandCursorJoined:
            documentsJoined.append(document)

    # pass on to next dag
        destination_collection.insert_many(documentsJoined)

    def join_task():
        client = MongoClient('localhost', 27017)
        mydb = client["Cell_Phones"]
        source_collection = mydb["extractAndClean"]
        destination_collection = mydb["join"]

        joinQ = {"$lookup": {"from": "review5core", "localField": "asin",
                             "foreignField": "asin", "as": "reviewList"}}
        aggPipeWithJoin = [joinQ]
        commandCursorJoined = source_collection.aggregate(aggPipeWithJoin)

        documentsJoined = []
        for document in commandCursorJoined:
            documentsJoined.append(document)

        documentsJsonString = json.dumps(
            documentsJoined, default=json_util.default)
        # # write list of dicts to file

    # pass on to next dag
        destination_collection.insert_many(documentsJoined)

    def generateNPC_task():
        client = MongoClient('localhost', 27017)
        mydb = client["Cell_Phones"]
        source_collection = mydb["join"]
        destination_collection = mydb["generateNPC"]

        testDocument = source_collection.find_one()
    # print(testDocument.items())

    # Test on first review
        testReview = testDocument['reviewList'][1]
    # print(testReview)

        def addNounPhrasesCountField(review):

            review['nounPhrasesCount'] = {}

            if 'reviewText' not in review:
                return

            testReviewText = review['reviewText']

            # Tokenising sentence
            word_tokenize = nltk.tokenize.word_tokenize
            tokens = word_tokenize(testReviewText)
        #     print(tokens)

            tagged_tokens = nltk.pos_tag(tokens)

        #     gram = ("NP: {<DT>?<JJ>*<NN>}")
        #     gram = r"""NP: {<RB.?>*<VB.?>*<NNP>+<NN>?}""" # more specific & effective noun phrase identifier

            gram = r"NP: {<DT|PRP\$>?<JJ>*<NN.*>+}"

            chunkParser = nltk.RegexpParser(gram)
            parsed_tree = chunkParser.parse(tagged_tokens)

            # print(parsed_tree)

            # print("SUBTREES ---------------------")

            # for tree in parsed_tree.subtrees():
            #     for leaf in tree.leaves():
            #         print(leaf)

            # each Leaf object contains [0] the word; and [1] the word category (eg. "something / NN")

            noun_phrases = [' '.join(leaf[0] for leaf in tree.leaves()).lower()  # have to lower here
                            # idk why if i lower earlier the parser fks up
                            for tree in parsed_tree.subtrees()
                            if tree.label() == 'NP']

        #     print(noun_phrases)

            counts = dict()
            for np in noun_phrases:
                counts[np] = counts.get(np, 0) + 1
            review['nounPhrasesCount'] = counts
        #     print(counts)
        #     print(review)

        # addNounPhrasesCountField(testReview)

        # print(testReview)

        # test on review 18 (looking for some normal output)
        reviewThree = testDocument['reviewList'][17]

        addNounPhrasesCountField(reviewThree)

        print(reviewThree)

        def gatherAllReviewsNPCount(productDocument, howMany=0):
            productReviews = productDocument['reviewList']
            productDocument['totalNounPhrasesCount'] = {}
            # just to make it shorter
            productDocumentCount = productDocument['totalNounPhrasesCount']
        #     for rev in productReviews:

        #     doneCount = 0
            if (howMany != 0):
                for x in range(howMany):
                    rev = productReviews[x]
                    addNounPhrasesCountField(rev)
                    # test and do for first 4 first

                    for np in rev['nounPhrasesCount']:
                        productDocumentCount[np] = productDocumentCount.get(
                            np, 0) + rev['nounPhrasesCount'][np]
            else:
                for rev in productReviews:
                    #             print("doing else once for " + str(doneCount))
                    #             doneCount += 1
                    addNounPhrasesCountField(rev)

                    for np in rev['nounPhrasesCount']:
                        productDocumentCount[np] = productDocumentCount.get(
                            np, 0) + rev['nounPhrasesCount'][np]

        #     print(productDocument)

        # print(testDocument)
        gatherAllReviewsNPCount(testDocument)
        # print(testDocument)

        cursor = source_collection.find({})

        documents = []
        for document in cursor:
            gatherAllReviewsNPCount(document)
            documents.append(document)

        # outputting
        documentsJsonString = json.dumps(documents, default=json_util.default)
        # # write list of dicts to file

        # pass on to next dag
        destination_collection.insert_many(documents)

    def generateSentiment_task():

        # Connect to MongoDB
        client = MongoClient('localhost', 27017)
        mydb = client["Cell_Phones"]  # CHANGE THIS LATER
        source_collection = mydb["generateNPC"]  # CHANGE THIS LATER
        destination_collection = mydb["generateSentiment"]

        # finds all documents (if needed, generateSentiment needs it)
        cursor = source_collection.find({})
        documents = []
        for document in cursor:
            documents.append(document)

        def addSentimentToReview(review, lyzer):

            if 'reviewText' not in review:
                return  # have to account for this later

            testReviewText = review['reviewText']

            score = lyzer.polarity_scores(testReviewText)

            review['sentiment'] = score

        def generateSentiment(productDocument, lyzer):
            reviewsList = productDocument['reviewList']

            averageSentiment = {'neg': 0.0, 'neu': 0.0,
                                'pos': 0.0, 'compound': 0.0}
            # need to keep count because some reviews dont have sentiment (because no reviewText)
            reviewsWithSentimentCount = 0

            for rev in reviewsList:

                addSentimentToReview(rev, lyzer)
                if 'sentiment' in rev:
                    reviewsWithSentimentCount += 1
                    for key in averageSentiment:
                        averageSentiment[key] += rev['sentiment'][key]

            for key in averageSentiment:
                if reviewsWithSentimentCount != 0:
                    averageSentiment[key] = averageSentiment[key] / \
                        reviewsWithSentimentCount

            productDocument['averageSentiment'] = averageSentiment

        lyzer = SentimentIntensityAnalyzer()
        for document in documents:
            generateSentiment(document, lyzer)

        destination_collection.insert_many(documents)

    # # outputting to JSON
    # documentsJsonString = json.dumps(documents, default=json_util.default)

    # # pass on to next dag
    # # # write list of dicts to file
    # with open('outputWithNPCountAndSentiment.json', 'w') as outfile:
    #     outfile.write(documentsJsonString)

    # def process_data(**kwargs):
    #     # Connect to MongoDB
    #     client = MongoClient('localhost', 27017)
    #     mydb = client["Cell_Phones"]
    #     my_collection = mydb["products"]

    #     ti = kwargs['ti']

    #     # Filtering dataset to remove unwanted elements
    #     result = mydb.phones.aggregate([
    #         {
    #             "$lookup": {
    #                 "from": "reviews",
    #                 "localField": "asin",
    #                 "foreignField": "asin",
    #                 "as": "reviews"
    #             }
    #         },
    #         {
    #             "$limit": 1
    #         }
    #     ])

    #     record = result.next()

    #     print(record)
    #     # Aggregate Queries
    #     # Remove all irrelevant fields
    #     q0 = {"$unset": ["fit", "tech1", "tech2", "imageURL", "imageURLHighRes"]}

    # # Match all documents that have "Cell Phones" string in category field
    #     q1 = {"$match": {"category": {"$in": ["Cell Phones"]}}}

    # # Match all documents that have some sort of ranking in the general Cell Phones & Accessories
    #     regexQ2 = re.compile(
    #         "^>#.* in Cell Phones & Accessories \(See Top 100 in Cell Phones & Accessories\)")
    #     q2 = {"$match": {"rank": {"$elemMatch": {"$regex": regexQ2}}}}

    # # Add a new field ranking the documents based on the "rank" string. Removes all hashtags and commas.
    #     regexQ3 = re.compile("[#,]")
    #     q3 = {"$addFields": {"overallRankRemovedHash": {"$replaceAll": {
    #         "input": {"$arrayElemAt": ["$rank", 0]}, "find": "#", "replacement": ""}}}}

    #     q3p1 = {"$addFields": {"overallRankRemovedRightArrow": {"$replaceAll": {
    #         "input": "$overallRankRemovedHash", "find": ">", "replacement": ""}}}}
    #     q3p2 = {"$addFields": {"overallRankRemovedComma": {"$replaceAll": {
    #         "input": "$overallRankRemovedRightArrow", "find": ",", "replacement": ""}}}}

    # # Now split by whitespace and only select first element (which is the number rank)
    #     q4 = {"$addFields": {"overallRankNumber": {"$toInt": {
    #         "$arrayElemAt": [{"$split": ["$overallRankRemovedComma", " "]}, 0]}}}}

    # # remove the intermediate "rankRemovedHashComma" (and also _id actually)
    #     q5 = {"$unset": ["overallRankRemovedHash",
    #                      "overallRankRemovedRightArrow", "overallRankRemovedComma", "_id"]}

    # # sort by rank ascending
    #     sortQ = {"$sort": {"overallRankNumber": 1}}

    #     # joinQ = {"$lookup": {"from": "reviews", "localField": "asin",
    #     #                      "foreignField": "asin", "as": "result"}}
    #     aggPipeWithJoin = [q0, q1, q2, q3, q3p1, q3p2, q4, q5, sortQ]
    #     commandCursorJoined = my_collection.aggregate(aggPipeWithJoin)

    #     documentsJoined = []
    #     for document in commandCursorJoined:
    #         documentsJoined.append(document)

    # # pass on to next dag
    #     with open('outputCleaned.json', 'w') as outfile:
    #         kwargs['ti'].xcom_push(key='outputCleaned', value=documentsJoined)
    #         json.dump(documentsJoined, outfile)

    #     # result = mydb.phones.aggregate([
    #     #     {
    #     #         "$lookup": {
    #     #             "from": "review5core",
    #     #             "localField": "asin",
    #     #             "foreignField": "asin",
    #     #             "as": "reviewList"
    #     #         }
    #     #     },
    #     #     {
    #     #         "$project": {
    #     #             "_id": 0,
    #     #             "asin": 1,
    #     #             "title": 1,
    #     #             "brand": 1,
    #     #             "feature": 1,
    #     #             "price": 1,
    #     #             "category": 1,
    #     #             "overallRankNumber": 1,
    #     #             "reviewList": 1
    #     #         }
    #     #     }
    #     # ])

    #     # record = result.next()

    #     # print(record)

    #     joinQ = {"$lookup": {"from": "review5core", "localField": "asin",
    #                          "foreignField": "asin", "as": "reviewList"}}
    #     aggPipeWithJoin = [joinQ]
    #     commandCursorJoined = my_collection.aggregate(aggPipeWithJoin)

    #     documentsJoined = []
    #     for document in commandCursorJoined:
    #         documentsJoined.append(document)

    #     documentsJsonString = json.dumps(
    #         documentsJoined, default=json_util.default)
    #     # # write list of dicts to file

    # # pass on to next dag
    #     with open('outputJoined.json', 'w') as outfile:
    #         outfile.write(documentsJsonString)

    # client = MongoClient('localhost', 27017)
    # mydb = client["Cell_Phones"]
    # joinedSampleCollection = mydb["cleanedReviewJoined"]

    #  testDocument = joinedSampleCollection.find_one()
    #   # print(testDocument.items())

    #   # Test on first review
    #   testReview = testDocument['reviewList'][1]
    #    # print(testReview)

    #    def addNounPhrasesCountField(review):

    #         review['nounPhrasesCount'] = {}

    #         if 'reviewText' not in review:
    #             return

    #         testReviewText = review['reviewText']

    #         # Tokenising sentence
    #         word_tokenize = nltk.tokenize.word_tokenize
    #         tokens = word_tokenize(testReviewText)
    #     #     print(tokens)

    #         tagged_tokens = nltk.pos_tag(tokens)

    #     #     gram = ("NP: {<DT>?<JJ>*<NN>}")
    #     #     gram = r"""NP: {<RB.?>*<VB.?>*<NNP>+<NN>?}""" # more specific & effective noun phrase identifier

    #         gram = r"NP: {<DT|PRP\$>?<JJ>*<NN.*>+}"

    #         chunkParser = nltk.RegexpParser(gram)
    #         parsed_tree = chunkParser.parse(tagged_tokens)

    #         # print(parsed_tree)

    #         # print("SUBTREES ---------------------")

    #         # for tree in parsed_tree.subtrees():
    #         #     for leaf in tree.leaves():
    #         #         print(leaf)

    #         # each Leaf object contains [0] the word; and [1] the word category (eg. "something / NN")

    #         noun_phrases = [' '.join(leaf[0] for leaf in tree.leaves()).lower()  # have to lower here
    #                         # idk why if i lower earlier the parser fks up
    #                         for tree in parsed_tree.subtrees()
    #                         if tree.label() == 'NP']

    #     #     print(noun_phrases)

    #         counts = dict()
    #         for np in noun_phrases:
    #             counts[np] = counts.get(np, 0) + 1
    #         review['nounPhrasesCount'] = counts
    #     #     print(counts)
    #     #     print(review)

    #     # addNounPhrasesCountField(testReview)

    #     # print(testReview)

    #     # test on review 18 (looking for some normal output)
    #     reviewThree = testDocument['reviewList'][17]

    #     addNounPhrasesCountField(reviewThree)

    #     print(reviewThree)

    #     def gatherAllReviewsNPCount(productDocument, howMany=0):
    #         productReviews = productDocument['reviewList']
    #         productDocument['totalNounPhrasesCount'] = {}
    #         # just to make it shorter
    #         productDocumentCount = productDocument['totalNounPhrasesCount']
    #     #     for rev in productReviews:

    #     #     doneCount = 0
    #         if (howMany != 0):
    #             for x in range(howMany):
    #                 rev = productReviews[x]
    #                 addNounPhrasesCountField(rev)
    #                 # test and do for first 4 first

    #                 for np in rev['nounPhrasesCount']:
    #                     productDocumentCount[np] = productDocumentCount.get(
    #                         np, 0) + rev['nounPhrasesCount'][np]
    #         else:
    #             for rev in productReviews:
    #                 #             print("doing else once for " + str(doneCount))
    #                 #             doneCount += 1
    #                 addNounPhrasesCountField(rev)

    #                 for np in rev['nounPhrasesCount']:
    #                     productDocumentCount[np] = productDocumentCount.get(
    #                         np, 0) + rev['nounPhrasesCount'][np]

    #     #     print(productDocument)

    #     # print(testDocument)
    #     gatherAllReviewsNPCount(testDocument)
    #     # print(testDocument)

    #     cursor = joinedSampleCollection.find({})

    #     documents = []
    #     for document in cursor:
    #         gatherAllReviewsNPCount(document)
    #         documents.append(document)

    #     # outputting
    #     documentsJsonString = json.dumps(documents, default=json_util.default)
    #     # # write list of dicts to file

    #     # pass on to next dag
    #     with open('outputWithNPCount.json', 'w') as outfile:
    #         outfile.write(documentsJsonString)

    # def addSentimentToReview(review, lyzer):

    #     if 'reviewText' not in review:
    #         return  # have to account for this later

    #     testReviewText = review['reviewText']

    #     score = lyzer.polarity_scores(testReviewText)

    #     review['sentiment'] = score

    # # before
    # # print(testReview)

    # lyzer = SentimentIntensityAnalyzer()
    # addSentimentToReview(testReview, lyzer)

    # # after
    # # print(testReview)

    # def generateSentiment(productDocument, lyzer):
    #     reviewsList = productDocument['reviewList']

    #     averageSentiment = {'neg': 0.0, 'neu': 0.0,
    #                         'pos': 0.0, 'compound': 0.0}
    #     # need to keep count because some reviews dont have sentiment (because no reviewText)
    #     reviewsWithSentimentCount = 0

    #     for rev in reviewsList:

    #         addSentimentToReview(rev, lyzer)
    #         if 'sentiment' in rev:
    #             reviewsWithSentimentCount += 1
    #             for key in averageSentiment:
    #                 averageSentiment[key] += rev['sentiment'][key]

    #     for key in averageSentiment:
    #         if reviewsWithSentimentCount != 0:
    #             averageSentiment[key] = averageSentiment[key] / \
    #                 reviewsWithSentimentCount

    #     productDocument['averageSentiment'] = averageSentiment

    # lyzer = SentimentIntensityAnalyzer()
    # generateSentiment(testDocument, lyzer)
    # # print(testDocument)

    # # test first doc
    # # documents[0]

    # for document in documents:
    #     generateSentiment(document, lyzer)

    # # outputting
    # documentsJsonString = json.dumps(documents, default=json_util.default)

    # # pass on to next dag
    # # # write list of dicts to file
    # with open('outputWithNPCountAndSentiment.json', 'w') as outfile:
    #     outfile.write(documentsJsonString)

    # # mydb = client["Cell_Phones"]
    # # my_collection = mydb["reviewNPCSentiment"]

    # # with open('outputWithNPCountAndSentiment.json', 'r') as f:
    # #     data = json.load(f)

    # # all_reviews = []

    # # for record in data:
    # #     for review in record['reviewList']:
    # #         all_reviews.append(review)

    # # # test print uncomment to view
    # # # for i in range(10):
    # # #     print(all_reviews[i])

    # # # This will throw an error (duplicate key) but it works
    # # converted = loads(json.dumps(all_reviews))
    # # my_collection.insert_many(converted)

    extractAndClean_task = PythonOperator(
        task_id='extractAndClean_task',
        python_callable=extractAndClean_task
    )

    join_task = PythonOperator(
        task_id='join_task',
        python_callable=join_task
    )

    generateNPC_task = PythonOperator(
        task_id='generateNPC_task',
        python_callable=generateNPC_task
    )

    generateSentiment_task = PythonOperator(
        task_id='generateSentiment_task',
        python_callable=generateSentiment_task
    )

    extractAndClean_task >> join_task >> generateNPC_task >> generateSentiment_task
