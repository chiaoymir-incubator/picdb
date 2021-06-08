import os
from os import walk
from pathlib import Path


def get_home_path():
    return str(Path.home())


def get_store_path(home_path):
    return os.path.join(home_path, '.picdb')


def get_dir_path(store_path):
    return os.path.join(store_path, 'images')


def get_config_path(home_path, store_path):
    return os.path.join(home_path, store_path, '.config')
