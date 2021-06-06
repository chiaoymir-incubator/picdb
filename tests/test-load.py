import os
from os import walk
import random
import pymongo
from pymongo import MongoClient
import base64
import bson
from bson.binary import Binary
import hashlib
from time import perf_counter


try:
    # establish a connection to the database
    connection = MongoClient('localhost', 27017)
    print("Successfully connected!")
except:
    print("Cannot connect to mongodb!")

# get a handle to the test database
db = connection.test_picdb


def test_find_all():
    count = 10
    coll = db.images

    start = perf_counter()

    for _ in range(count):

        result = coll.find({}, {"_id": 0, "img_id": 1})

        result = list(result)

    stop = perf_counter()

    print(f'Time: {(stop - start) / count}')


def test_find_random():
    count = 10
    size = 280000
    coll = db.images

    seq = [random.randint(0, size) for _ in range(1000)]

    start = perf_counter()

    for _ in range(count):

        result = coll.find({"img_id": {"$in": seq}}, {
                           "_id": 0, "content": 1, "img_id": 1})

        # result = list(map(lambda x: x["img_id"], result))
        result = list(result)

    stop = perf_counter()

    print(f'Time: {(stop - start) / count}')


def test_find_all_images():
    count = 10
    coll = db.images

    start = perf_counter()

    for _ in range(count):

        result = coll.find({"uploader": "Eric", "img_type": "jpg", "use_count": 0, "tags": ["image"]}, {
                           "_id": 0, "img_id": 1})

        # result = list(map(lambda x: x["img_id"], result))

    stop = perf_counter()

    print(f'Time: {(stop - start) / count}')


if __name__ == "__main__":
    # test_find_all()
    test_find_random()
    # test_find_all_images()
