import os
import io
from io import BytesIO
from os import walk

from pymongo import MongoClient
from bson.objectid import ObjectId
from PIL import Image
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


from utils.dir_utils import get_home_path, get_store_path, get_dir_path

from utils.cache_utils import create_cache_dir
from utils.cache_utils import make_cache_info, get_cache_info, check_cache_info
from utils.cache_utils import get_next_cache_version
from utils.cache_utils import store_downloaded_cache, read_downloaded_cache

from utils.db_utils import get_images_id_from_database, get_images_content_from_database

from utils.image_utils import save_image, get_downloaded_images_list, get_image_path


class PicDB:
    def __init__(self):
        self.db_name = 'picdb'
        self.db_collection = 'images'
        self.db_log_collection = 'logs'
        self.threshold = 100
    def init(self, uri='mongodb://localhost:27017/'):
        """Initialize database connection and member variable setup"""
        self.home_path = get_home_path()
        self.store_path = get_store_path(self.home_path)
        self.dir_path = get_dir_path(self.store_path)
        if not os.path.isdir(self.store_path):
            os.mkdir(self.store_path)

        if not os.path.isdir(self.dir_path):
            os.mkdir(self.dir_path)

        self.uri = uri

        # Establish a connection to the database
        try:
            self.connection = MongoClient(self.uri)
            print("Successfully connected!\n")
        except:
            print("Cannot connect to mongodb!")

        self.db = self.connection[self.db_name]
        self.collection = self.db[self.db_collection]
        self.log_collection = self.db[self.db_log_collection]

    def upload_one_new_image(self, img_path, up_loader, tags_list_like=[], description="null"):

        # get one image
        im = Image.open(img_path)

        # convert the image to binary
        image_bytes = io.BytesIO()
        im.save(image_bytes, format=im.format)

        if self.collection.find_one({"content": image_bytes.getvalue()}) != None:
            print("Already exist!")
            return

        # create initial credits for tags
        credits_for_tags = {}
        logs_for_new_image = []
        if len(tags_list_like) != 0:
            for i in tags_list_like:
                if i not in credits_for_tags.keys():
                    credits_for_tags[i] = 1
        else:
            credits_for_tags["image"] = 1

        # create one record (row) for table
        image = {
            "content": image_bytes.getvalue(),
            "description": description,
            "img_type": str(im.format),
            "use_count": 0,
            "uploader": up_loader,
            "tags": credits_for_tags
        }

        # insert the data into the collection
        image_id = self.collection.insert_one(image).inserted_id
        print("upload ", img_path.split("/")[-1], " is done!")

        #update logs
        for tag in credits_for_tags:
            self.log_collection.insert_one({'tag':tag, 'user':up_loader, '_id':image_id})

    def upload_file_of_new_images(self, img_file_path, up_loader, tags_list_like=[], img_type=[], description="null"):
        allImagesList = os.listdir(img_file_path)
        if img_file_path[-1] != "/":
            img_file_path = img_file_path + "/"

        for img in allImagesList:
            s = img.split('.')
            if s[-1] in img_type:
                self.upload_one_new_image(img_file_path + img, up_loader, tags_list_like, description)


    def show_image(self, image_id):
        documents = self.collection.find({'_id': ObjectId(image_id)})
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

    def show_information(self, image_id):
        documents = self.collection.find({"_id": ObjectId(image_id)},{"content": 0})
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
        fig = plt.figure(figsize=(8, 4))
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
        
    def feedback_folder(self, user, tags, filepath, positive_feedback):
        raw_files = [f for f in listdir(filepath) if isfile(join(filepath, f))]
        
        files = []
        for file in raw_files:
            item = file.split('.')[0]
            if item != '': files += [item]

        self.feedback(user, [tags], files, positive_feedback)

    def feedback_all_folder(self, user, filepath, positive_feedback):
        raw_path = [f for f in listdir(filepath) if isdir(join(filepath, f))]
        for path in raw_path:
            self.feedback_folder(user, path, join(filepath, path), positive_feedback)

    def feedback(self, user, tags, ids, positive_feedback):
        
        for tag in tags:
            if not (tag in self.db.list_collection_names()):
                self.db.create_collection(tag)

            buf = self.db[tag]
            entity = 'tags.%s' % tag
            value = 1 if positive_feedback else -1
            ids = [ObjectId(id) for id in ids]

            self.collection.update_many({'_id' : {'$in': ids}}, {'$inc':{ entity : value}}, upsert=True)

            if positive_feedback:
                result = self.collection.find({'_id' : {'$in' : ids}, entity : {'$gt' : self.threshold-1}})
                self.log_collection.update_many({'_id' : {'$in' : ids}}, {'$set':{'tag':tag, 'user': user}}, upsert=True)

                delete_data, insert_data = [], []

                for item in result:
                    delete_data += [item['_id']]
                    insert_data += [{'_id' : item['_id']}]
                print(insert_data)
                if len(delete_data) != 0 :
                    buf.delete_many({'_id' : {'$in' : delete_data}})
                if len(insert_data) != 0 :
                    buf.insert_many(insert_data)

            else:
                result = self.collection.find({'_id' : {'$in' : ids}, entity : {'$lt' : self.threshold}})

                delete_data = []

                for item in result:
                    delete_data += [item['_id']]

                if len(delete_data) == 0 : return 
                buf.delete_many({'_id' : {'$in' : delete_data}})
        return



    def get_images(self, tags, img_type="JPEG", use_count=-1,
                   limit=10, use_cache=True, cache_version=0,
                   next_cache_name="latest"):
        """
        Get Image from database

        Parameters:
        ----------
        tags: List[String]
            the provided tags to select images from database

        img_type: "jpg" or "png"
            specify the type of the images

        use_count: Int
            the use count of the selected images

        limit: Int
            the maximal images number to return

        use_cache: Bool
            True: Use local cache instead of querying database
            False: Query database for updated information

        cache_version: Int
            if specified to use local cache, you have to specify a version to use

        next_cache_name: String
            the cache label for the next cache version

        """
        # Force user to give a label for the next cache version
        if not use_cache and next_cache_name == "latest":
            print("You have to specify a label name for new cache version!")
            return

        cache_tag_dir = create_cache_dir(self.dir_path, tags)
        version, name, cache_exists = check_cache_info(
            self.dir_path, tags, str(cache_version))

        cache_path = os.path.join(
            cache_tag_dir, f'{version}-{name}.config')

        if not use_cache or not cache_exists:
            # Get images from the database
            images_list = get_images_id_from_database(
                self.db, tags, img_type, use_count, limit)

            print("Getting images list from database!")

        else:
            # Read from local cache
            cached_info = read_downloaded_cache(cache_path)
            cached_list = cached_info["images_list"]

            print("Reading Cached list!")
            print(f"{len(cached_list)} images cached!\n")

            # Not enough cache images, find more from database
            if(len(cached_list) < limit):
                images_list = get_images_id_from_database(
                    self.db, tags, img_type, use_count, limit)
            else:
                images_list = cached_list

        # Get the list of all downloaded images
        downloaded_list = get_downloaded_images_list(self.dir_path)

        # Filter out downloaded images to only download not downloaded ones
        if downloaded_list:
            to_download_list = list(set(images_list) - set(downloaded_list))

        else:
            to_download_list = images_list

        print(f'Find {len(to_download_list)} images to download!\n')

        # Actually retrieve undownloaded images
        download_images = get_images_content_from_database(
            self.db, to_download_list)

        # Save images based on their ids
        for image in download_images:
            save_image(self.dir_path, image)

        # Store latest cache info
        latest_cache_path = os.path.join(cache_tag_dir, '0-latest.config')
        cache_info = make_cache_info(
            images_list, tags, img_type, use_count, limit)
        store_downloaded_cache(cache_info, latest_cache_path)

        # Save new cache version in local cache diretory
        if not use_cache:
            next_cache_version = get_next_cache_version(self.dir_path, tags)
            new_cache_path = os.path.join(
                cache_tag_dir, f'{next_cache_version}-{next_cache_name}.config')
            cache_info = make_cache_info(
                images_list, tags, img_type, use_count, limit)

            store_downloaded_cache(cache_info, new_cache_path)

    def use_images(self, tags, cache_version=0):
        """Get images list for given tags and cache version"""

        version, name, cache_exists = check_cache_info(
            self.dir_path, tags, str(cache_version))

        if not cache_exists:
            print(f'Version: {version} -- Name: {name} not found!\n')
            print('Please download the images first!')
            return

        cache_tag_dir = create_cache_dir(self.dir_path, tags)
        cache_path = os.path.join(
            cache_tag_dir, f'{version}-{name}.config')

        cache_info = read_downloaded_cache(cache_path)
        cached_list = cache_info["images_list"]

        return cached_list

    def move_images(self, tags, dst_path, relative=True, cache_version=0):
        """Move downloaded images of a cache version to the given user directory"""

        # Get cache info
        cache_info = get_cache_info(self.dir_path, tags, cache_version)
        images_list = cache_info["images_list"]
        img_type = cache_info["img_type"]

        # Convert relative path to absolute path
        if relative:
            dst_path = os.path.abspath(dst_path)
        print(dst_path)
        _, _, filenames = next(walk(dst_path))

        # Find images to move
        moved_list = [filename.split('.')[0] for filename in filenames]

        to_move_list = list(set(images_list) - set(moved_list))

        print(f"Find {len(to_move_list)} images to move!")

        # Move downloaded images to destination directory for further user own usage
        for image_name in to_move_list:
            img_path = f'{image_name}.{img_type}'
            src_image_path = get_image_path(self.dir_path, img_path)
            dst_image_path = os.path.join(dst_path, img_path)
            try:
                with open(src_image_path, "rb") as f:
                    encoded = f.read()

                with open(dst_image_path, "wb") as f:
                    f.write(encoded)

            except FileNotFoundError:
                print(f'Image: {image_name} not found!')

if __name__ == '__main__':
    pic_db = PicDB()
    pic_db.init()
