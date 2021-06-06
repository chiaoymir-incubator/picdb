import pymongo
from pymongo import MongoClient
import base64
import bson
from bson.binary import Binary
from pprint import pprint

try:
    # establish a connection to the database
    connection = MongoClient('localhost', 27017)
    print("Successfully connected!")
except:
    print("Cannot connect to mongodb!")


# get a handle to the test database
db = connection.test_image
# file_meta = db.file_meta
file_saved = "../test-store/test.jpg"


def main():
    coll = db.images
    image = coll.find_one({"description": "test"})

    with open(file_saved, "wb") as f:
        f.write(image['file'])


main()
