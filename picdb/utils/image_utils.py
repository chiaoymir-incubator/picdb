import os
from os import walk


def save_image(dir_path, image):
    image_id = str(image['_id'])
    image_type = image['img_type']
    path = os.path.join(
        dir_path, f'{image_id}.{image_type}')
    print(
        f'Downloading image: {image_id}.{image_type} ...')
    with open(path, "wb") as f:
        f.write(image["content"])


def get_downloaded_images_list(dir_path):
    _, _, filenames = next(walk(dir_path))

    images_list = [filename.split('.')[0] for filename in filenames]

    return images_list


def get_image_path(dir_path, image_name):
    return os.path.join(dir_path, image_name)
