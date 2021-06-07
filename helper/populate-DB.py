import os
from os import walk
import random
from pymongo import MongoClient
from bson.binary import Binary
import string


try:
    # establish a connection to the database
    connection = MongoClient('localhost', 27017)
    print("Successfully connected!")
except:
    print("Cannot connect to mongodb!")

# get a handle to the test database
db = connection.picdb

# Image test
paths = [
    os.path.abspath('../dataset/images/anime'),
    os.path.abspath('../dataset/images/cats'),
    os.path.abspath('../dataset/images/dogs'),
    os.path.abspath('../dataset/images/pokemon')
]

# Main tag
tag_name = ['anime', 'cat', 'dog', 'pokemon']

# Random uploader
uploaders = ['Alice', 'Bob', 'Cindy', 'David',
             'Eric', 'Frank', 'Greg', 'Higg', 'Ivy', 'Jenny']

# Additional tags for further test
anime_tags = ['girl', 'JK', 'colorful', 'black',
              'megane', 'long-haired', 'short-haired']
cat_tags = ['orange', 'black', 'white', 'spot',  'munchkin']
dog_tags = ['black', 'white', 'puppy', 'shiba']
pokemon_tags = ['fire', 'water', 'mega', 'normal', 'unknown', 'legend']


def get_random_string(length):
    # choose from all lowercase letter
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))

    return result_str


def main():
    # count = 0
    images_list = []
    coll = db.images

    for i, path in enumerate(paths):
        # Get all files name for each path
        _, _, filenames = next(walk(path))

        # Add absoulte path prefix to each file
        for idx, filename in enumerate(filenames):
            filenames[idx] = os.path.join(path, filename)

        main_tag = tag_name[i]
        more_tags = []

        if i == 0:
            more_tags = anime_tags
        elif i == 1:
            more_tags = cat_tags
        elif i == 2:
            more_tags = dog_tags
        elif i == 3:
            more_tags = pokemon_tags

        for filename in filenames:
            type = filename.split('.')[-1]
            uploader = uploaders[random.randint(0, 9)]

            # Choose some additional tags
            more_tags_num = random.randint(1, len(more_tags))
            # Sample tags from more tags list
            choices = random.sample(more_tags, more_tags_num)

            choices.append(main_tag)

            tags = choices

            credits = dict()
            # Get random credits for each tags
            for tag in tags:
                credits[tag] = random.randint(-20, 100)

            description = get_random_string(10)
            use_count = random.randint(0, 100)

            with open(filename, "rb") as f:
                encoded = Binary(f.read())

            print(f'Adding {filename} to uploading list...')

            doc = {"content": encoded, "description": description, "logs": [
            ], "type": type, "use_count": use_count, "uploader": uploader, "tags": tags, "credits": credits}

            images_list.append(doc)

    # Randomize images in the list
    random.shuffle(images_list)
    # Insert all read image into the databases
    print('Uploading to database ...')
    coll.insert_many(images_list)


if __name__ == "__main__":
    main()
