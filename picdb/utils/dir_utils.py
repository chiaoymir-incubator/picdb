import os
from pathlib import Path


def get_home_path():
    """Get user home path"""
    return str(Path.home())


def get_store_path(home_path):
    """Get user internal image store path"""
    return os.path.join(home_path, '.picdb')


def get_dir_path(store_path):
    """Get image store images directory path"""
    return os.path.join(store_path, 'images')
