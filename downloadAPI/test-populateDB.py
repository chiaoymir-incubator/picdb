import os
from os import walk
import random
import pymongo
from pymongo import MongoClient
import base64
import bson
from bson.binary import Binary
import hashlib


mypath = os.path.abspath("../test-image")
_, _, filenames = next(walk('../test-image'))

for idx, filename in enumerate(filenames):
    filenames[idx] = os.path.join(mypath, filename)

seq = list(range(0, len(filenames)))
random.shuffle(seq)

credits = [random.randint(0, 100) for _ in range(len(filenames))]

# for i in seq:
#     print(filenames[i])

# print(len(seq))

try:
    # establish a connection to the database
    connection = MongoClient('localhost', 27017)
    print("Successfully connected!")
except:
    print("Cannot connect to mongodb!")

# get a handle to the test database
db = connection.picdb


def main():
    coll = db.images
    for idx, val in enumerate(seq):
        filename = filenames[val]
        type = filename.split('.')[-1]
        with open(filename, "rb") as f:
            encoded = Binary(f.read())

        doc = {"img_id": str(idx), "filename": filename, "content": encoded, "description": "test image file",
               "logs": [], "img_type": type, "use_count": 0, "uploader": "Eric", "tags": {"image": credits[idx]}}

        coll.insert_one(doc)


if __name__ == "__main__":
    main()
