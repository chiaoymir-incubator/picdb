import os
from os import walk
import random
import pymongo
from pymongo import MongoClient
import base64
import bson
from bson.binary import Binary
import hashlib


try:
    # establish a connection to the database
    connection = MongoClient('localhost', 27017)
    print("Successfully connected!")
except:
    print("Cannot connect to mongodb!")

# get a handle to the test database
db = connection.test_picdb

# store_path = os.path.abspath("../dataset/images")

paths = [
    os.path.abspath('../dataset/images/anime'),
    os.path.abspath('../dataset/images/cats'),
    os.path.abspath('../dataset/images/dogs'),
    os.path.abspath('../dataset/images/pokemon')
]


def main():
    count = 0

    for path in paths:
        _, _, filenames = next(walk(path))

        for idx, filename in enumerate(filenames):
            filenames[idx] = os.path.join(path, filename)

        seq = list(range(0, len(filenames)))
        random.shuffle(seq)

        # credits = [random.randint(0, 100) for _ in range(len(filenames))]random.randint(0, 100)

        coll = db.images

        for idx, val in enumerate(seq):
            filename = filenames[val]
            type = filename.split('.')[-1]
            with open(filename, "rb") as f:
                encoded = Binary(f.read())

            print(f'Uploading {filename} ...')

            doc = {"img_id": str(count), "filename": filename, "content": encoded, "description": "test image file", "logs": [
            ], "img_type": type, "use_count": 0, "uploader": "Eric", "tags": ["image"], "credits": {"image": random.randint(0, 100)}}

            coll.insert_one(doc)
            count += 1


if __name__ == "__main__":
    main()
