from pymongo import MongoClient
try:
    connection = MongoClient('mongodb://localhost:27017/')
    print("Connection successful")
except:
    print("Unsuccessful")


# db = connection['test']
# doc = {"test": "success"}
# db['test'].insert_one(doc)
