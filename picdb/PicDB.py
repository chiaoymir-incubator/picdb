import os
from os import walk

from pymongo import MongoClient

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


if __name__ == '__main__':
    # pic_db = PicDB()
    # pic_db.init()
    # pic_db.get_images(limit=150)
    # pic_db.close_connection()

    # tags = ["cat"]
    # tags = ["orange"]
    tags = ["cat", 'orange']
    limit = 20
    name = "exp3"
    version = 3
    dst_path = "../test-image"
    relative = True
    cache_version = 1

    # ==== testing
    # test_get_images(tags, limit)
    # test_set_new_version(tags, name, limit)
    test_use_cache_version(tags, version)
    # test_move_images(tags, dst_path, relative, cache_version)
