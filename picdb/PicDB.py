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

    def get_images(self, tags=["cats"], img_type="jpg", use_count=-1, limit=100, use_cache=True, create_cache=True, cache_version=0, next_cache_name="latest"):
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

        cache_tag_dir = self._create_cache_dir(tags)
        version, name, cache_exists = self._check_cache_info(
            tags, cache_version)
        next_version = version + 1

        # Cache not found, get images from the database
        if not use_cache or not cache_exists:
            # Find image id by given conditions
            images_list = self._get_images_id_from_database(
                tags, img_type, use_count, limit)

            print("Getting images list from database!")

        else:
            # Read from local cache
            cache_path = os.path.join(
                cache_tag_dir, f'{version}-{name}.config')

            cached_list = self._read_downloaded_cache(cache_path)

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
            print(f'Find {len(to_download_list)} images to downloads!\n')

        else:
            to_download_list = images_list

        # print(to_download_list)

        # Actually retrieve undownloaded images
        download_images = self._get_images_content_from_database(
            to_download_list)

        # print(len(download_images))

        # Save images based on their ids
        for image in download_images:
            image_id = str(image['_id'])
            image_type = image['img_type']
            path = os.path.join(
                self.dir_path, f'{image_id}.{image_type}')
            print(
                f'Downloading image: {image_id}.{image_type} ...')
            with open(path, "wb") as f:
                f.write(image["content"])

        # Save cache ids in local database
        if create_cache:
            self._store_downloaded_cache(images_list, cache_path)

        # TODO: Increment credits 1 for each image

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
                print(f'Find cache version: {version} -- name: {name}')
                return version, name, True

        # Not found, return the latest version
        return 0, "latest", False

    def get_all_cache_version(self, tags):
        """
        Cache file name is of version-name.config convention
        """
        tags_path = '-'.join(sorted(tags))
        cache_tag_dir = os.path.join(
            self.dir_path, 'cache', tags_path)

        sorted_dir_list = sorted([path.split('.')[0]
                                  for path in os.listdir(cache_tag_dir)])

        version_name_list = [path.split('-') for path in sorted_dir_list]

        return version_name_list

    def list_all_cache_version(self, tags):
        version_name_list = self.get_all_cache_version(tags)

        tags_label = ' '.join(tags)
        if len(version_name_list) == 0:
            print(f'No Cache for [{tags_label}] now!')
            return

        print(f"Cache for [{tags_label}]: ")
        for version, name in version_name_list:
            print(f'Version: {version} -- Name: {name}')

    def _create_latest_config(self, cache_tag_dir):
        cache_file_path = os.path.join(cache_tag_dir, 'latest.config')

        with open(cache_file_path, "wb") as f:
            pickle.dump([], f)

    def _get_images_id_from_database(self, tags, img_type, use_count, limit):
        # TODO: add code to query server index first
        # TODO: server may implement several level indexing
        images = self.collection.find(
            {"tags": {"$all": tags},
             "img_type": img_type,
             "use_count": {"$gt": use_count}},
            {"img_id": 1}).limit(limit)

        images_list = [str(image['_id']) for image in images]

        return images_list

    def _get_images_content_from_database(self, images_list):
        id_list = [ObjectId(id) for id in images_list]
        images = self.collection.find(
            {"_id": {"$in": id_list}}, {"img_id": 1, "content": 1, "img_type": 1})

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

    def use_images(self, step=1):
        pass

    def check_connection(self, uri):
        # Establish a connection to the database
        try:
            self.connection = MongoClient(uri)
            print("Successfully connected!\n")
        except:
            print("Cannot connect to mongodb!")


def test_download():
    pass

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


def test_cache_list():
    pic_db = PicDB()
    pic_db.init()
    pic_db.list_all_cache_version(["cats", "orange"])
    # pic_db.list_all_cache_version(["orange", "cats"])


if __name__ == '__main__':
    # pic_db = PicDB()
    # pic_db.init()
    # pic_db.get_images(limit=150)
    # pic_db.close_connection()
    test_cache_list()
