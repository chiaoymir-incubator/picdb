import os
from os import walk

from pymongo import MongoClient

from utils.dir_utils import get_home_path, get_store_path, get_dir_path, get_config_path

from utils.cache_utils import create_cache_dir, get_cache_dir, get_cache_tag_dir, get_cache_file_path
from utils.cache_utils import get_metadata_path, get_cache_path, get_etl_path
from utils.cache_utils import make_cache_info, get_cache_info, check_cache_info
from utils.cache_utils import get_all_cache_version, list_all_cache_version, get_next_cache_version
from utils.cache_utils import store_downloaded_cache, read_downloaded_cache

from utils.db_utils import get_images_id_from_database, get_images_content_from_database

from utils.image_utils import save_image, get_downloaded_images_list, get_image_path


class PicDB:
    def __init__(self):
        self.db_name = 'picdb'
        self.db_collection = 'images'

    def init(self, uri='mongodb://localhost:27017/'):
        """Initialize database connection and config setup
        config example:
            home_path: /Users/chiao1
            store_path: /Users/chiao1/.picdb
            dir_path: /Users/chiao1/.picdb/images
            uri: mongodb://localhost:27017/
        """
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

    def get_images(self, tags, img_type="jpg", use_count=-1,
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

        threshold: Int
            the credit threshold to select from database
            (may support list of number later)

        use_count: Int
            the use count of the selected images

        update: Bool
            if update is true, the method will always fetch from database instead of using local cache list

        """
        if not use_cache and next_cache_name == "latest":
            print("You have to specify a label name for new cache version!")
            return

        # if not use_cache and cache_version == 0:
        #     print("You need to specify ")

        cache_tag_dir = create_cache_dir(self.dir_path, tags)
        version, name, cache_exists = check_cache_info(
            self.dir_path, tags, str(cache_version))

        cache_path = os.path.join(
            cache_tag_dir, f'{version}-{name}.config')

        # Cache not found, get images from the database
        if not use_cache or not cache_exists:
            # Find image id by given conditions
            images_list = get_images_id_from_database(
                self.db, tags, img_type, use_count, limit)

            print("Getting images list from database!")

        else:
            # Read from local cache
            cached_info = read_downloaded_cache(cache_path)
            cached_list = cached_info["images_list"]

            print("Reading Cached list!")
            print(f"{len(cached_list)} images cached!\n")

            if(len(cached_list) < limit):
                images_list = get_images_id_from_database(
                    self.db, tags, img_type, use_count, limit)
            else:
                images_list = cached_list

        # print(images_list)

        # Compare with the downloaded list (all images)
        downloaded_list = get_downloaded_images_list(self.dir_path)

        # print(downloaded_list)

        # Filter out downloaded images
        if downloaded_list:
            to_download_list = list(set(images_list) - set(downloaded_list))

        else:
            to_download_list = images_list

        print(f'Find {len(to_download_list)} images to download!\n')

        # print(to_download_list)

        # Actually retrieve undownloaded images
        download_images = get_images_content_from_database(
            self.db, to_download_list)

        # print(len(download_images))

        # Save images based on their ids
        for image in download_images:
            save_image(self.dir_path, image)

        # Store
        latest_cache_path = os.path.join(cache_tag_dir, '0-latest.config')
        cache_info = make_cache_info(
            images_list, tags, img_type, use_count, limit)
        store_downloaded_cache(cache_info, latest_cache_path)

        # Save cache ids in local database
        if not use_cache:
            next_cache_version = get_next_cache_version(self.dir_path, tags)
            new_cache_path = os.path.join(
                cache_tag_dir, f'{next_cache_version}-{next_cache_name}.config')
            cache_info = make_cache_info(
                images_list, tags, img_type, use_count, limit)

            store_downloaded_cache(cache_info, new_cache_path)

    def close_connection(self):
        print('Closing Database connection ...')
        self.connection.close()

    def check_connection(self, uri):
        # Establish a connection to the database
        try:
            self.connection = MongoClient(uri)
            print("Successfully connected!\n")
        except:
            print("Cannot connect to mongodb!")

    def preprocess_images(self, tags, width=180, height=180, etl_version=0):
        """
        Process image to an uniform format
        """
        pass

    def use_images(self, tags, cache_version=0):
        """
        Get images list
        """
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
        """
        Move images to the given directory
        """
        cache_info = get_cache_info(self.dir_path, tags, cache_version)
        images_list = cache_info["images_list"]
        img_type = cache_info["img_type"]

        # Convert relative path to absolute path
        if relative:
            dst_path = os.path.abspath(dst_path)

        _, _, filenames = next(walk(dst_path))

        moved_list = [filename.split('.')[0] for filename in filenames]

        to_move_list = list(set(images_list) - set(moved_list))

        print(f"Find {len(to_move_list)} images to move!")

        # Move downloaded images to destination directory for further usage
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
                # print(f'You may have to redownload it through download function!')


# Other TODOs:
# Implement versioning cache on the client side
# Compute hash for each upload to prevent duplicate images
# Add 2 level index in database to improve data quality, should put all document in a collection
# We may first check the number of images downloaded before asking databases
# Can try to publish this api as a pypi package
# Up vote mechanism to improve visibility of new images
# May consider to wrap setup environment in docker
# Do some visualization features
# Add a cli interface
# Add complete documentation of this apis
# Implement use_images for user to use
# Use image can let user to enter how many they want to use per steop
# Allow user to save cache file (maybe)

# Possible to do
# Extract part of code that are able to find whether the images had beed downloaded
# This function can be reused for download, move, and etl pipeline
# Refactor code to reduce instance methods
# Write a static method for create db connection


def test_get_images(tags, limit):
    pic_db = PicDB()
    pic_db.init()
    pic_db.get_images(tags, limit=limit)


def test_set_new_version(tags, name, limit):
    pic_db = PicDB()
    pic_db.init()
    # Get images from databases
    pic_db.get_images(tags, use_cache=False,
                      next_cache_name=name, limit=limit)


def test_use_cache_version(tags, cache_version):
    pic_db = PicDB()
    pic_db.init()
    pic_db.get_images(tags, cache_version=cache_version)


def test_move_images(tags, dst_path, relative, cache_version):
    pic_db = PicDB()
    pic_db.init()
    pic_db.move_images(tags, dst_path, relative, cache_version)


# TODO:
# 1. Add multiple tags for images and insert into the database -- complete
# 2. Test multiple tags cache versioning -- complete
# 3. Write code to query index in database first then the default image pool -- complete
# 4. Write use_image function for user to load images -- complete
# 5. Write data pipeline code to lead to cache in local index (ETL)
# 6. Write pipeline visualization code for user-definded ETL processes
#    Ex: show a version of cache that has several tags
# X. Write a lot of test codes

# ETL pipeline part
# - A user can build an user-defined pipeline, which inputs are our cache file
# - We can define several special function like resize, and then store result to ETL images
# - For example, it can mix several cache-file from different tags
# - Or, in a more advanced scenario, the user will generate users' own image
#   , then he can put this images under an ETL directory, just like our images one
#   , the functionality of this directory is to act as a custom image pool
#   , the user can then upload these images to the data lake
# - To complete this function, we will ask user for a unique label, every photos
#   that user generated will be put under this directory to prevent mixing
# - User may have the ability to define several stage to manipulate data based on these labels
# - We may also store some config, for example, the parent set of this images set
# - Later, we can plot the relationship between each etl sets

# Visualization part
# - Based on cached label list, draw information based on credits or something to show the
# distribution of this cached file
# - We may use matplotlib or some plotting library to plot our result
# - The goal is to let user can easily view what he had downloaded
# - The total information from the default image pool is implemented by others


if __name__ == '__main__':
    # pic_db = PicDB()
    # pic_db.init()
    # pic_db.get_images(limit=150)
    # pic_db.close_connection()

    # tags = ["cat"]
    # tags = ["orange"]
    tags = ["cat", 'orange']
    limit = 30
    name = "exp3"
    version = 2
    dst_path = "../test-image"
    relative = True
    cache_version = 3

    # ==== testing
    # test_cache_list()
    # test_get_images(tags, limit)
    # test_set_new_version(tags, name, limit)
    # test_use_cache_version(tags, version)
    test_move_images(tags, dst_path, relative, cache_version)
