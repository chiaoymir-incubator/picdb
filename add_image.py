import pymongo
import bson.binary
from pymongo import MongoClient
from io import BytesIO
client = MongoClient('localhost', 27017)

dblist = client.list_database_names()
db = client['demo']
# create collection
coll = db.image
image_id = [str(i) for i in range(2, 100)]
img_name = '1.png'
for i in range(len(image_id)):
    with open (img_name,'rb') as myimage:
        content = BytesIO(myimage.read())
        coll.insert_one(dict(
            img_id = image_id[i],
            filename = img_name,
            content = bson.binary.Binary(content.getvalue()),
            description = "test image file",
            logs = [],
            img_type = "png",
            use_count = 0,
            uploader = "admin",
            tags = [],
            credits = {}
        ))
print("Add image successfully !")