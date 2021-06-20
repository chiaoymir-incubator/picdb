from pymongo import MongoClient, IndexModel, TEXT
from bson.objectid import ObjectId
from os import listdir
from os.path import isfile, join, isdir
class Feedback:

	def __init__(self, client, database, collection):
		self.client = client
		self.database = client[database]
		self.collection = self.database[collection]
		self.refresh_buffer = self.database['refresh_buffer']
		return

	def single_tag_query(self, tag):
		result = set()
		ptr = self.database[tag].find({})
		for item in ptr:
			result.add(item['_id'])
		return result

	def multi_tag_query(self, tags):
		num = len(tags)
		result = self.single_tag_query(tags[0])
		for i in range(1, num):
			result = result & self.single_tag_query(tags[i])
		return list(result)
		
	def feedback_folder(self, tags, filepath, positive_feedback):
		raw_files = [f for f in listdir(filepath) if isfile(join(filepath, f))]
		
		files = []
		for file in raw_files:
			item = file.split('.')[0]
			if item != '': files += [item]

		self.feedback([tags], files, positive_feedback)

	def feedback_all_folder(self, filepath, positive_feedback):
		raw_path = [f for f in listdir(filepath) if isdir(join(filepath, f))]
		for path in raw_path:
			self.feedback_folder(path, join(filepath, path), positive_feedback)

	def feedback(self, tags, ids, positive_feedback):
		
		for tag in tags:
			if not (tag in self.database.list_collection_names()):
				self.database.create_collection(tag)

			buf = self.database[tag]
			entity = 'credits.%s' % tag
			value = 1 if positive_feedback else -1
			ids = [ObjectId(id) for id in ids]
			self.collection.update_many({'_id' : {'$in': ids}}, {'$inc':{ entity : value}}, upsert=True)

			if positive_feedback:
				result = self.collection.find({'_id' : {'$in' : ids}, entity : {'$gt' : 99}})

				delete_data, insert_data = [], []

				for item in result:
					delete_data += [item['_id']]
					insert_data += [{'_id' : item['_id']}]

				if len(delete_data) == 0 : return 

				buf.delete_many({'_id' : {'$in' : delete_data}})
				buf.insert_many(insert_data)

			else:
				result = self.collection.find({'_id' : {'$in' : ids}, entity : {'$lt' : 100}})

				delete_data = []

				for item in result:
					delete_data += [item['_id']]

				buf.delete_many({'_id' : {'$in' : delete_data}})

		return