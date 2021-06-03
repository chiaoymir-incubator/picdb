import pymongo
from pymongo import MongoClient
import base64
import bson
from bson.binary import Binary

try:
    # establish a connection to the database
    connection = MongoClient('localhost', 27017)
    print("Successfully connected!")
except:
    print("Cannot connect to mongodb!")


# get a handle to the test database
db = connection.test_image
file_meta = db.file_meta
file_used = "../dataset/images/cats/cat.1.jpg"


def main():
    coll = db.images
    with open(file_used, "rb") as f:
        encoded = Binary(f.read())

    coll.insert_one(
        {"filename": file_used, "file": encoded, "description": "test"})


main()
