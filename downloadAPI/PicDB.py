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
            print("Successfully connected!")
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

        print(f'Config: --------')
        print(f'home_path: {self.home_path}')
        print(f'store_path: {self.store_path}')
        print(f'dir_path: {self.dir_path}')
        print(f'uri: {self.uri}')
        print()

    def get_images(self, tags=["cats"], img_type="jpg", threshold=0, use_count=0, update=False, create_cache=True):
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
        cache_path = '.' + '-'.join(sorted(tags)) + '.config'
        cache_exists = os.path.isfile(cache_path)

        if update or not cache_exists:
            # Find image id by given conditions
            images_list = self._get_images_id_from_database(
                tags, img_type, threshold, use_count)

        else:
            # Read from local cache
            # For example, .cats.config, .cats-orange.config
            images_list = self._read_downloaded_cache(cache_path)

        # Compare with the downloaded list
        downloaded_list = self._get_dir_images_list()

        # Filter out downloaded images
        if downloaded_list:
            to_download_list = list(set(images_list) - set(downloaded_list))

        else:
            to_download_list = images_list

        # Actually retrieve undownloaded images
        download_images = self._get_images_content_from_database(
            to_download_list)

        # Save images based on their ids
        for image in download_images:
            path = os.path.join(
                self.dir_path, image["img_id"] + '.' + image["img_type"])
            print(
                f'Downloading image: {image["img_id"]}.{image["img_type"]} ...')
            with open(path, "wb") as f:
                f.write(image["content"])

        # Save cache ids in local database
        if create_cache:
            all_images_list = list(set(images_list) | set(to_download_list))
            self._store_downloaded_cache(all_images_list, cache_path)

    def _get_images_id_from_database(self, tags=["cats"], img_type="jpg", threshold=0, use_count=0):
        # TODO: add code to query server index first
        # TODO: server may implement several level indexing
        images = self.collection.find(
            {"tags": {"$all": tags},
             "img_type": img_type,
             "use_count": {"$gt": use_count}},
            {"img_id": 1,
             "_id": 0})

        images_list = [image['img_id'] for image in images]

        return images_list

    def _get_images_content_from_database(self, images_list):
        images = self.collection.find(
            {"img_id": {"$in": images_list}}, {"img_id": 1, "content": 1, "img_type": 1, "_id": 0})

        images_list = [image for image in images]

        return images_list

    def _store_downloaded_cache(self, store_cache, prefix):
        cache_path = os.path.join(self.dir_path, 'config')
        if not os.path.isdir(cache_path):
            os.mkdir(cache_path)

        cache_file_path = os.path.join(cache_path, prefix + '.config')
        with open(cache_file_path, "wb") as f:  # Pickling
            pickle.dump(store_cache, f)

    def _read_downloaded_cache(self, prefix):
        cache_path = os.path.join(self.dir_path, 'config')
        if not os.path.isdir(cache_path):
            return

        cache_file_path = os.path.join(cache_path, prefix + '.config')
        if not os.path.isfile(cache_file_path):
            return

        with open(cache_file_path, "rb") as f:
            downloaded_cache = pickle.load(f)

        return downloaded_cache

    def _get_dir_images_list(self):
        _, _, filenames = next(walk(self.dir_path))

        images_list = [filename.split('.')[0] for filename in filenames]

        return images_list


def test_download():
    pass


if __name__ == '__main__':
    pic_db = PicDB()
    pic_db.init()
    # pic_db.get_images()
