import os
from os import walk
import random
import pymongo
from pymongo import MongoClient
import base64
import bson
from bson.binary import Binary
import hashlib
import string


try:
    # establish a connection to the database
    connection = MongoClient('localhost', 27017)
    print("Successfully connected!")
except:
    print("Cannot connect to mongodb!")

# get a handle to the test database
db = connection.picdb

# store_path = os.path.abspath("../dataset/images")

paths = [
    os.path.abspath('../dataset/images/anime'),
    os.path.abspath('../dataset/images/cats'),
    os.path.abspath('../dataset/images/dogs'),
    os.path.abspath('../dataset/images/pokemon')
]

tag_name = ['anime', 'cats', 'dogs', 'pokemon']

uploaders = ['Alice', 'Bob', 'Cindy', 'David',
             'Eric', 'Frank', 'Greg', 'Higg', 'Ivy', 'Jenny']


def get_random_string(length):
    # choose from all lowercase letter
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))

    return result_str


def main():
    count = 0

    for i, path in enumerate(paths):
        _, _, filenames = next(walk(path))

        for idx, filename in enumerate(filenames):
            filenames[idx] = os.path.join(path, filename)

        seq = list(range(0, len(filenames)))
        random.shuffle(seq)

        tags = [tag_name[i]]

        coll = db.images

        for idx, val in enumerate(seq):
            filename = filenames[val]
            type = filename.split('.')[-1]
            uploader = uploaders[random.randint(0, 9)]
            credits_arr = [random.randint(0, 100) for _ in range(len(tags))]
            credits = dict()
            description = get_random_string(20)

            for i in range(len(tags)):
                credits[tags[i]] = credits_arr[i]

            with open(filename, "rb") as f:
                encoded = Binary(f.read())

            print(f'Uploading {filename} ...')

            doc = {"img_id": str(count), "filename": filename, "content": encoded, "description": description, "logs": [
            ], "img_type": type, "use_count": 0, "uploader": uploader, "tags": tags, "credits": credits}

            coll.insert_one(doc)
            count += 1


if __name__ == "__main__":
    main()
