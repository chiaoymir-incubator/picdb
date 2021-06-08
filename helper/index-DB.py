from pymongo import MongoClient

try:
    # establish a connection to the database
    connection = MongoClient('localhost', 27017)
    print("Successfully connected!")
except:
    print("Cannot connect to mongodb!")

# get a handle to the test database
db = connection.picdb

image_coll = db.images


# tags = ["cat"]
# limit = 20
# use_count = 20

tags = ["cat"]
# tags = ["orange"]
limit = 1000
use_count = 20

index_name = '-'.join(sorted(tags))

result = image_coll.find(
    {"tags": {"$all": tags}, "use_count": {"$gt": use_count}}, {"img_type": 1, "use_count": 1, "uploader": 1, "tags": 1, "credits": 1}).limit(limit)

# {"img_type": 1, "use_count": 1,"uploader": 1, "tags": 1, "credits": 1}
# print(len(list(result)))

coll = db.cat
# coll = db.orange
index_list = list(result)
# print(index_list)
coll.insert_many(index_list)
# for image in index_list:
#     coll.insert_one(image)

# result = coll.find({})

# print(list(result))
