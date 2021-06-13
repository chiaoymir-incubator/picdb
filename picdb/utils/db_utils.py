from bson.objectid import ObjectId


def get_images_id_from_database(db, tags, img_type, use_count, limit):
    """
    Get image id list from database given some conditions.

    Parameters:
    ----------
    tags: 
        the tags to select image

    img_type:
        jpg or png

    use_count:
        the use count threshould for this query

    limit:
        the maximal images number to return

    Returns:
    ----------
    images_list:
        the images list given those conditions

    """
    # First find intersection of each tag index in the databases
    all_tags_list = []

    for tag in tags:
        coll = db[tag]

        result = coll.find(
            {"tags": {"$all": [tag]},
             "img_type": img_type,
             "use_count": {"$gt": use_count}},
            {"_id": 1}).limit(limit)

        images = [str(image['_id']) for image in list(result)]

        all_tags_list.append(images)

    id_list = set(all_tags_list[0]).intersection(*all_tags_list[1:])

    # Second, check whether we have to fetch more images from default image pool
    if len(id_list) == limit:
        print("Get images from index!")
        images_list = id_list

    else:
        print("Not enough images from index!")
        print("Try to find more in the default image pool!")
        coll = db.images
        # Then to find other images from the default image pool
        images = coll.find(
            {"tags": {"$all": tags},
             "img_type": img_type,
             "use_count": {"$gt": use_count}},
            {"_id": 1, 'content': 0}).limit(limit)

        images_list = [str(image['_id']) for image in images]

    return images_list


def get_images_content_from_database(db, images_list):
    """Get images content based on given image id list"""
    id_list = [ObjectId(id) for id in images_list]
    coll = db.images
    images = coll.find(
        {"_id": {"$in": id_list}}, {"content": 1, "img_type": 1})

    images_list = [image for image in images]

    return images_list
