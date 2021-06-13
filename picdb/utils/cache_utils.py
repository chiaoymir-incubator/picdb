import os
import pickle


def create_cache_dir(dir_path, tags):
    """Create default cache directory"""
    # Make sure the main cache directory exists
    # Create cache directory -- .picdb/images/cache
    cache_dir = get_cache_dir(dir_path)

    if not os.path.isdir(cache_dir):
        os.mkdir(cache_dir)

    # Make sure the tags cache directory exists
    # Create cache directory -- ex: .picdb/images/cache/cats-orange/
    cache_tag_dir = get_cache_tag_dir(cache_dir, tags)

    if not os.path.isdir(cache_tag_dir):
        os.mkdir(cache_tag_dir)

    return cache_tag_dir


def get_cache_dir(dir_path):
    """Get main root cache directoy path"""
    return os.path.join(dir_path, 'cache')


def get_cache_tag_dir(cache_dir, tags):
    """Get tags cache directory path"""
    return os.path.join(cache_dir, '-'.join(sorted(tags)))


def get_metadata_path(dir_path, tags, path):
    return os.path.join(dir_path, path, '-'.join(sorted(tags)))


def get_cache_path(dir_path, tags):
    return get_metadata_path(dir_path, tags, "cache")


def get_etl_path(dir_path, tags):
    return get_metadata_path(dir_path, tags, 'etl')


def get_cache_file_path(dir_path, tags, version, name):
    """Get the cache file path given tags, version and name"""
    cache_tag_dir = get_cache_path(dir_path, tags)
    cache_path = os.path.join(
        cache_tag_dir, f'{version}-{name}.config')

    return cache_path


def make_cache_info(images_list, tags, img_type, use_count, limit):
    """Make cache info object and return"""
    cache_info = {
        "images_list": images_list,
        "tags": tags,
        "img_type": img_type,
        "use_count": use_count,
        "limit": limit
    }

    return cache_info


def get_cache_info(dir_path, tags, cache_version):
    """
    Get cache information provided tags and cache_version

    Returns:
    ==========
    downloaded_cache: dict
        includes images id list, tags, img_type, use_count and limit

    """
    version_name_list = get_all_cache_version(dir_path, tags)

    for version, name in version_name_list:
        if version == str(cache_version):
            cache_path = os.path.join(
                dir_path, 'cache', '-'.join(sorted(tags)), f'{version}-{name}.config')

            with open(cache_path, "rb") as f:
                downloaded_cache = pickle.load(f)

            return downloaded_cache

    return {}


def check_cache_info(dir_path, tags, cache_version):
    """Use version to find cache, if exist, return that version, else default to 0-latest"""
    version_name_list = get_all_cache_version(dir_path, tags)

    for version, name in version_name_list:
        # Find a matched version
        if version == cache_version:
            # print(f'Find cache version: {version} -- name: {name}\n')
            return version, name, True

    # Not found, return the latest version
    return 0, "latest", False


def get_all_cache_version(dir_path, tags):
    """Get a version-label list of cache name for given tags"""
    try:
        tags_path = '-'.join(sorted(tags))
        cache_tag_dir = os.path.join(
            dir_path, 'cache', tags_path)

        sorted_dir_list = sorted([path.split('.')[0]
                                  for path in os.listdir(cache_tag_dir)])

        version_name_list = [path.split('-') for path in sorted_dir_list]

        return version_name_list
    except FileNotFoundError:
        print("You had never download images!")
        return


def list_all_cache_version(dir_path, tags):
    """List all cache version for some given tags"""
    try:
        version_name_list = get_all_cache_version(dir_path, tags)

        tags_label = ' '.join(tags)
        if len(version_name_list) == 0:
            print(f'No Cache for [{tags_label}] now!')
            return

        print(f"Cache for [{tags_label}]: ")
        for version, name in version_name_list:
            print(f'Version: {version} -- Name: {name}')
    except TypeError:
        print(f'You had never create cache for these tags!')


def get_next_cache_version(dir_path, tags):
    """Get the next cache version, which is the next integer of the newest cache version in the directory now"""
    version_name_list = get_all_cache_version(dir_path, tags)

    return len(version_name_list)


def store_downloaded_cache(store_cache, cache_file_path):
    """Store cache on given path"""
    with open(cache_file_path, "wb") as f:  # Pickling
        pickle.dump(store_cache, f)


def read_downloaded_cache(cache_file_path):
    """Read downloaded cache on given path"""
    if not os.path.isfile(cache_file_path):
        return []

    with open(cache_file_path, "rb") as f:
        downloaded_cache = pickle.load(f)

    return downloaded_cache
