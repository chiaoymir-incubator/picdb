from enum import auto
import pymongo
import bson.binary
from pymongo import MongoClient
from io import BytesIO
from PIL import Image
import keyboard
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

class ImageDB(object):
    def __init__(self, db_name, image_coll, log_coll):
        self.client = MongoClient('localhost', 27017)
        self.db = self.client[db_name]
        self.collection = self.db[image_coll]
        self.log_collection = self.db[log_coll]

    def insert_tag(self):
        print("Select image ID to insert the tag : ", end='')
        image_id = input()
        tag = input("Input the tag : ")
        user = input("Who are you: ")
        print()
        print("Please check the tagging information")
        print("Image ID : {}".format(image_id))
        print("Tagging : {}".format(tag))
        print("User : {}".format(user))
        print("Press Y to continue the tagging or press N to cancel")
        while True:
            if keyboard.is_pressed("y"):
                image_id = image_id.split(' ')
                for id in image_id:
                    documents = self.collection.find({'img_id':id})
                    result = list(documents)
                    if len(result) == 0:
                        print("Image ID {} is not exist !".format(id))
                        continue
                    tagging = result[0]['tags']
                    log = result[0]['logs']
                    log.append("add|{}|{}".format(tag, user))
                    tagging.append(tag)
                    myquery = { "img_id": id }
                    add_tag = { "$set": { "tags": tagging } }
                    update_log = { "$set": { "logs": log } }
                    self.collection.update_one(myquery, add_tag)
                    self.collection.update_one(myquery, update_log)
                    self.log_collection.insert_one({'tag':tag, 'user':user, 'image_id':id})
                    print("Image ID {} is tagged successfully！".format(id))
                print()
                break
            elif keyboard.is_pressed("n"):
                print("Cancel the tagging")
                print()
                break

    def show_image(self):
        image_id = input("Select image ID to visualize : ")
        documents = self.collection.find({'img_id':image_id})
        result = list(documents)
        if len(result) == 0:
            print("Image ID is not exist !")
            print()
        else:
            image_data = result[0]['content']
            image = Image.open(BytesIO(image_data))
            plt.imshow(image)
            plt.axis('off')
            plt.show()
            print()

    def show_information(self):
        image_id = input("Select image ID : ")
        documents = self.collection.find({"img_id": image_id},{"content": 0})
        result = list(documents)
        if len(result) == 0:
            print("Image ID is not exist !")
        else:
            print(result[0])
            print()

    def show_summary(self):
        # TODO: tag_visualize, user 
        total_image = self.collection.count_documents({})
        top_num = 3
        fig = plt.figure()
        fig.suptitle('There are {} images in database'.format(total_image))
        ax1 = fig.add_subplot(121)
        ax2 = fig.add_subplot(122)
        all_tag = self.log_collection.distinct('tag')
        all_user = self.log_collection.distinct('user')
        # calculate the number of each tag
        tag_count = []
        for tag in all_tag:
            tag_num = self.log_collection.count_documents({'tag': tag})
            tag_count.append([tag, tag_num])
        tag_count = sorted(tag_count, key=lambda x: x[1], reverse=True)
        tag_count = pd.DataFrame(tag_count[:top_num], columns=['Tag_name', 'Tag_count'])
        sns.barplot(x='Tag_name', y='Tag_count', data=tag_count, ax=ax1)
        ax1.set_title('Top {} tags in database'.format(top_num), fontsize=8)

        # calculate the number of the user who add the tag
        user_count = []
        for user in all_user:
            user_num = self.log_collection.count_documents({'user': user})
            user_count.append([user, user_num])
        user_count = sorted(user_count, key=lambda x: x[1], reverse=True)
        user_count = pd.DataFrame(user_count[:top_num], columns=['User_name', 'Number'])
        sns.barplot(x='User_name', y='Number', data=user_count, ax=ax2)
        ax2.set_title('Top {} users to insert tag'.format(top_num), fontsize=8)
        plt.show()
        print()

    def show_instruction(self):
        print("Press s to show the image from image_id")
        print("Press i to insert the tag of image")
        print("Press p to see the information of the image")
        print("Press a to show the summary of the database")
        print("Press q to quit")

    def get_command(self):
        while True:
            if keyboard.is_pressed("s"):
                self.show_image()
                self.show_instruction()
            elif keyboard.is_pressed("i"):
                self.insert_tag()
                self.show_instruction()
            elif keyboard.is_pressed("p"):
                self.show_information()
                self.show_instruction()
            elif keyboard.is_pressed("a"):
                self.show_summary()
                self.show_instruction()
            elif keyboard.is_pressed("q"):
                break
            
def main():
    db_name = 'demo'
    image_coll = 'image'
    log_coll = 'log'
    db = ImageDB(db_name, image_coll, log_coll)
    db.show_instruction()
    db.get_command()

if __name__ == '__main__':
    main()