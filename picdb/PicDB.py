import os
from os import walk
import random
import base64
from pathlib import Path
import configparser
import pickle

import pymongo
from pymongo import MongoClient
import bson
from bson.binary import Binary
from bson.objectid import ObjectId


class PicDB:
    def __init__(self):
        self.db_name = 'picdb'
        self.db_collection = 'images'

    def init(self, dir_path="images", uri='mongodb://localhost:27017/'):
        """Initialize database connection and config setup
        config example:
            home_path: /Users/chiao1
            store_path: /Users/chiao1/.picdb
            dir_path: /Users/chiao1/.picdb/images
            uri: mongodb://localhost:27017/
        """
        home_path = str(Path.home())
        store_path = '.picdb'
        config_path = os.path.join(home_path, store_path, '.config')

        # Create or read config
        if not os.path.isfile(config_path):
            self._init_config_path(home_path, store_path, dir_path)
            self._init_connection(uri)
            self._init_config_file()

        else:
            self._read_config(config_path)
            self._connect(self.uri)

    def _init_config_path(self, home_path, store_path, dir_path):
        self._init_home(home_path)
        self._init_store(store_path)
        self._init_dir(dir_path)

    def _init_home(self, path):
        self.home_path = path

    def _init_store(self, path):
        store_path = os.path.join(self.home_path, path)
        self.store_path = store_path
        if not os.path.isdir(store_path):
            os.mkdir(store_path)

    def _init_dir(self, path):
        dir_path = os.path.join(self.store_path, path)
        self.dir_path = dir_path
        if not os.path.isdir(dir_path):
            os.mkdir(dir_path)

    def _init_config_file(self):
        config = configparser.ConfigParser()
        config['path'] = {
            'home_path': self.home_path,
            'store_path': self.store_path,
            'dir_path': self.dir_path
        }

        config['connection'] = {
            'uri': self.uri
        }
        config_path = os.path.join(self.store_path, '.config')

        with open(config_path, 'w') as f:
            config.write(f)

    def _init_connection(self, path):
        # Establish a connection to the database
        self.uri = path
        self._connect(self.uri)

    def _connect(self, path):
        # Establish a connection to the database
        try:
            self.connection = MongoClient(path)
            print("Successfully connected!\n")
        except:
            print("Cannot connect to mongodb!")

        self.db = self.connection[self.db_name]
        self.collection = self.db[self.db_collection]

    def set_config(self, path=".config"):
        if True:
            pass

    def _read_config(self, config_path):
        config = configparser.ConfigParser()
        config.read(config_path)

        self.home_path = config['path']['home_path']
        self.store_path = config['path']['store_path']
        self.dir_path = config['path']['dir_path']
        self.uri = config['connection']['uri']

        if not os.path.isdir(self.dir_path):
            os.mkdir(self.dir_path)

        # print(f'Config: --------')
        # print(f'home_path: {self.home_path}')
        # print(f'store_path: {self.store_path}')
        # print(f'dir_path: {self.dir_path}')
        # print(f'uri: {self.uri}')
        # print()

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

        cache_tag_dir = self._create_cache_dir(tags)
        version, name, cache_exists = self._check_cache_info(
            tags, str(cache_version))

        cache_path = os.path.join(
            cache_tag_dir, f'{version}-{name}.config')

        # Cache not found, get images from the database
        if not use_cache or not cache_exists:
            # Find image id by given conditions
            images_list = self._get_images_id_from_database(
                tags, img_type, use_count, limit)

            print("Getting images list from database!")

        else:
            # Read from local cache
            cached_info = self._read_downloaded_cache(cache_path)
            cached_list = cached_info["images_list"]

            print("Reading Cached list!")
            print(f"{len(cached_list)} images cached!\n")

            if(len(cached_list) < limit):
                images_list = self._get_images_id_from_database(
                    tags, img_type, use_count, limit)
            else:
                images_list = cached_list

        # print(images_list)

        # Compare with the downloaded list (all images)
        downloaded_list = self._get_downloaded_images_list()

        # print(downloaded_list)

        # Filter out downloaded images
        if downloaded_list:
            to_download_list = list(set(images_list) - set(downloaded_list))

        else:
            to_download_list = images_list

        print(f'Find {len(to_download_list)} images to download!\n')

        # print(to_download_list)

        # Actually retrieve undownloaded images
        download_images = self._get_images_content_from_database(
            to_download_list)

        # print(len(download_images))

        # Save images based on their ids
        for image in download_images:
            image_id = str(image['_id'])
            image_type = image['type']
            path = os.path.join(
                self.dir_path, f'{image_id}.{image_type}')
            print(
                f'Downloading image: {image_id}.{image_type} ...')
            with open(path, "wb") as f:
                f.write(image["content"])

        # Store
        latest_cache_path = os.path.join(cache_tag_dir, '0-latest.config')
        cache_info = self._make_cache_info(
            images_list, tags, img_type, use_count, limit)
        self._store_downloaded_cache(cache_info, latest_cache_path)

        # Save cache ids in local database
        if not use_cache:
            next_cache_version = self._get_next_cache_version(tags)
            new_cache_path = os.path.join(
                cache_tag_dir, f'{next_cache_version}-{next_cache_name}.config')
            cache_info = self._make_cache_info(
                images_list, tags, img_type, use_count, limit)

            self._store_downloaded_cache(cache_info, new_cache_path)

    def _make_cache_info(self, images_list, tags, type, use_count, limit):
        cache_info = {
            "images_list": images_list,
            "tags": tags,
            "type": type,
            "use_count": use_count,
            "limit": limit
        }

        return cache_info

    def _get_cache_info(self, tags, cache_version):
        version_name_list = self.get_all_cache_version(tags)

        for version, name in version_name_list:
            if version == str(cache_version):
                cache_path = os.path.join(
                    self.dir_path, 'cache', '-'.join(sorted(tags)), f'{version}-{name}.config')

                with open(cache_path, "rb") as f:
                    downloaded_cache = pickle.load(f)

                return downloaded_cache

        return {}

    def _create_cache_dir(self, tags):
        # Make sure the main cache directory exists
        # Create cache directory -- .picdb/images/cache
        cache_dir = os.path.join(self.dir_path, 'cache')

        if not os.path.isdir(cache_dir):
            os.mkdir(cache_dir)

        # Make sure the tags cache directory exists
        # Create cache directory -- ex: .picdb/images/cache/cats-orange/
        cache_tag_dir = os.path.join(cache_dir, '-'.join(sorted(tags)))

        if not os.path.isdir(cache_tag_dir):
            os.mkdir(cache_tag_dir)

        self.cache_dir = cache_dir
        self.cache_tag_dir = cache_tag_dir

        return cache_tag_dir

    def _check_cache_info(self, tags, cache_version):
        """
        Cache file name is of version-name.config convention

        Use version to find cache, cache name is only for usability
        """
        version_name_list = self.get_all_cache_version(tags)

        for version, name in version_name_list:
            # Find a matched version
            if version == cache_version:
                # print(f'Find cache version: {version} -- name: {name}\n')
                return version, name, True

        # Not found, return the latest version
        return 0, "latest", False

    def _get_next_cache_version(self, tags):
        version_name_list = self.get_all_cache_version(tags)

        return len(version_name_list)

    def get_all_cache_version(self, tags):
        """
        Cache file name is of version-name.config convention
        """
        try:
            tags_path = '-'.join(sorted(tags))
            cache_tag_dir = os.path.join(
                self.dir_path, 'cache', tags_path)

            sorted_dir_list = sorted([path.split('.')[0]
                                      for path in os.listdir(cache_tag_dir)])

            version_name_list = [path.split('-') for path in sorted_dir_list]

            return version_name_list
        except FileNotFoundError:
            print("You had never download images!")
            return

    def list_all_cache_version(self, tags):
        try:
            version_name_list = self.get_all_cache_version(tags)

            tags_label = ' '.join(tags)
            if len(version_name_list) == 0:
                print(f'No Cache for [{tags_label}] now!')
                return

            print(f"Cache for [{tags_label}]: ")
            for version, name in version_name_list:
                print(f'Version: {version} -- Name: {name}')
        except TypeError:
            print(f'You had never create cache for these tags!')

    def _create_latest_config(self, cache_tag_dir):
        cache_file_path = os.path.join(cache_tag_dir, 'latest.config')

        with open(cache_file_path, "wb") as f:
            pickle.dump([], f)

    def _get_images_id_from_database(self, tags, type, use_count, limit):
        index_name = '-'.join(sorted(tags))
        coll = self.db[index_name]

        result = coll.find(
            {"type": type,
             "use_count": {"$gt": use_count}},
            {"img_id": 1}).limit(limit)

        id_list = list(result)

        if len(id_list) == limit:
            print("Get images from index!")
            images_list = [str(image['_id']) for image in id_list]

        else:
            print("Not enough images!")
            print("Try to find more in the image pool!")
            # Then to find other images from the default image pool
            images = self.collection.find(
                {"tags": {"$all": tags},
                 "type": type,
                 "use_count": {"$gt": use_count}},
                {"_id": 1, 'content': 0}).limit(limit)

            images_list = [str(image['_id']) for image in images]

        return images_list

    def _get_images_content_from_database(self, images_list):
        id_list = [ObjectId(id) for id in images_list]
        images = self.collection.find(
            {"_id": {"$in": id_list}}, {"content": 1, "type": 1})

        images_list = [image for image in images]

        return images_list

    def _store_downloaded_cache(self, store_cache, cache_file_path):
        with open(cache_file_path, "wb") as f:  # Pickling
            pickle.dump(store_cache, f)

    def _read_downloaded_cache(self, cache_file_path):
        if not os.path.isfile(cache_file_path):
            return []

        with open(cache_file_path, "rb") as f:
            downloaded_cache = pickle.load(f)

        return downloaded_cache

    def _get_downloaded_images_list(self):
        _, _, filenames = next(walk(self.dir_path))

        images_list = [filename.split('.')[0] for filename in filenames]

        return images_list

    def _check_images_in_dir(self, images_list):
        pass

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

    def _get_metadata_path(self, tags, path):
        return os.path.join(self.dir_path, path, '-'.join(sorted(tags)))

    def _get_cache_path(self, tags):
        return self._get_metadata_path(tags, "cache")

    def _get_etl_path(self, tags):
        return self._get_metadata_path(tags, 'etl')

    def preprocess_images(self, tags, width=180, height=180, etl_version=0):
        """
        Process image to an uniform format
        """
        pass

    def _get_cache_file_path(self, tags, version, name):
        cache_tag_dir = self.get_cache_path(tags)
        cache_path = os.path.join(
            cache_tag_dir, f'{version}-{name}.config')

        return cache_path

    def use_images(self, tags, cache_version=0):
        """
        Get images list
        """
        version, name, cache_exists = self._check_cache_info(
            tags, str(cache_version))

        if not cache_exists:
            print(f'Version: {version} -- Name: {name} not found!\n')
            print('Please download the images first!')
            return

        cache_tag_dir = self._create_cache_dir(tags)
        cache_path = os.path.join(
            cache_tag_dir, f'{version}-{name}.config')

        cache_info = self._read_downloaded_cache(cache_path)
        cached_list = cache_info["images_list"]

        return cached_list

    def _get_image_path(self, image_name):
        return os.path.join(self.dir_path, image_name)

    def move_images(self, tags, dst_path, relative=True, cache_version=0):
        """
        Move images to the given directory
        """
        cache_info = self._get_cache_info(tags, cache_version)
        images_list = cache_info["images_list"]
        img_type = cache_info["type"]

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
            src_image_path = self._get_image_path(img_path)
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


def test_cache_list():
    pic_db = PicDB()
    pic_db.init()
    pic_db.list_all_cache_version(["cat", "orange"])
    # pic_db.list_all_cache_version(["orange", "cats"])


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


if __name__ == '__main__':
    # pic_db = PicDB()
    # pic_db.init()
    # pic_db.get_images(limit=150)
    # pic_db.close_connection()

    tags = ["cat", 'orange']
    limit = 25
    name = "exp3"
    version = 1
    dst_path = "../test-image"
    relative = True
    cache_version = 3

    # ==== testing
    # test_cache_list()
    # test_get_images(tags, limit)
    # test_set_new_version(tags, name, limit)
    # test_use_cache_version(tags, version)
    test_move_images(tags, dst_path, relative, cache_version)
