import unittest
import imghdr
import os
from os import walk
import random
from pymongo import MongoClient
from bson.binary import Binary
from bson.objectid import ObjectId
import string


def initialize_connection():
    try:
        # establish a connection to the database
        connection = MongoClient('localhost', 27017)
        # print("Successfully connected!")
    except:
        print("Cannot connect to mongodb!")

    return connection


class TestUpload(unittest.TestCase):

    def test_find_by_id(self):
        """
        Test we can get image by given id
        """
        conn = initialize_connection()
        db = conn.picdb
        coll = db.images

        id = "60bde182282b5d082ef019eb"

        self.assertTrue(coll.find({"_id": ObjectId(id)}, {"content": 0}))

    def test_upload_count(self):
        """
        Test whether the total image is 72389
        """
        conn = initialize_connection()
        db = conn.picdb
        coll = db.images

        num = coll.count_documents({})

        self.assertEqual(num, 72389)


if __name__ == '__main__':
    unittest.main()
