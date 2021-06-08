import matplotlib.pyplot as plt

from pymongo import MongoClient
from PIL import Image
import io
import os

# connect to data base (need to change andy_test_image)
client = MongoClient()
db = client.andy_test_image

# chang to a collection (table, need to change images)
images_collection = db.images


def upload_one_new_image(img_path, up_loader, tags_list_like=[], description="null"):

    # get one image
    im = Image.open(img_path)

    # convert the image to binary
    image_bytes = io.BytesIO()
    im.save(image_bytes, format=im.format)

    # create initial credits for tags and create initial logs for new image
    credits_for_tags = {}
    logs_for_new_image = []
    if len(tags_list_like) != 0:
        for i in tags_list_like:
            if i not in credits_for_tags.keys():
                credits_for_tags[i] = 1
            else:
                credits_for_tags[i] = credits_for_tags[i] + 1
            logs_for_new_image.append("add|" + i + "|" + up_loader)
    else:
        credits_for_tags["image"] = 1
        logs_for_new_image.append("add|image|" + up_loader)

    # create one record (row) for table
    image = {
        "filename": img_path.split("/")[-1],
        "content": image_bytes.getvalue(),
        "description": description,
        "logs": logs_for_new_image,
        "img_type": str(im.format),
        "use_count": 0,
        "uploader": up_loader,
        "tags": tags_list_like,
        "credits": credits_for_tags
    }

    # insert the data into the collection
    image_id = images_collection.insert_one(image).inserted_id
    print("upload ", img_path.split("/")[-1], " is done!")


def upload_file_of_new_images(img_file_path, up_loader, tags_list_like=[], description="null"):
    allImagesList = os.listdir(img_file_path)
    if img_file_path[-1] != "/":
        img_file_path = img_file_path + "/"

    for img in allImagesList:
        upload_one_new_image(img_file_path + img, up_loader,
                             tags_list_like, description)


# upload_file_of_new_images(
#     "./images2", "Andy", ["andy", "young"], "myself but younger")

# upload_one_new_image("./images/5.jpg", "Andy",
#                      ["room", "white"], "my restroom")
