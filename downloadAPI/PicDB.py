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

    def download(self, tags=["images"], img_type="jpg"):
        images = self.collection.find(
            {"tags.image": {"$exists": True, "$gt": 50}, "img_type": img_type}, {"img_id": 1, "_id": 0})

        images_list = set(map(lambda x: x['img_id']))
        config = self._load_download_config()
        downloaded_list = config['list']

        # Compare with downloaded list
        if downloaded_list:
            download_list = filter(
                lambda x: x not in downloaded_list, images_list)
        else:
            download_list = images_list

        images = self.collection.find(
            {"img_id": {}}, {"img_id": 1, "content": 1, "_id": 0})

        for image in images:
            path = os.path.join(
                self.dir_path, image["img_id"] + '.' + image["img_type"])
            print(
                f'Downloading image: {image["img_id"]}.{image["img_type"]} ...')
            with open(path, "wb") as f:
                f.write(image["content"])

    def _store_download_config(self, store_config):
        config_path = os.path.join(self.dir_path, '.config')
        with open("store.config", "wb") as f:  # Pickling
            pickle.dump(store_config, f)

    def _load_download_config(self):
        config_path = os.path.join(self.dir_path, '.config')

        if not os.path.isfile(config_path):
            return dict()

        with open(config_path, "rb") as f:   # Unpickling
            read_config = pickle.load(f)

        return read_config


if __name__ == '__main__':
    pic_db = PicDB()
    pic_db.init()
    pic_db.download()
