from pymongo import MongoClient, IndexModel, TEXT
from os import listdir
from os.path import isfile, join
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
			result.add(item['img_id'])
		return result

	def multi_tag_query(self, tags):
		num = len(tags)
		result = self.single_tag_query(tags[0])
		for i in range(1, num):
			result = result & self.single_tag_query(tags[i])
		return list(result)
		
	def feedback_folder(self, tags, filepath, positive_feedback):
		files = [f for f in listdir(filepath) if isfile(join(filepath, f))]
		print(files)
	def feedback(self, tags, ids, positive_feedback):
		
		#self.database.drop_collection(tag)
		for tag in tags:
			if not (tag in self.database.list_collection_names()):
				self.database.create_collection(tag)
				print("rere")
				index = IndexModel([('img_id', TEXT)], unique = True, dropDups=1)
				self.database[tag].create_indexes([index])

			buf = self.database[tag]
			entity = 'credits.%s' % tag
			value = 1 if positive_feedback else -1
				
			self.collection.update({'img_id' : {'$in': ids}}, {'$inc':{ entity : value}})

			if positive_feedback:
				result = self.collection.find({'img_id' : {'$in' : ids}, entity : {'$gt' : 99}})

				delete_data, insert_data = [], []

				for item in result:
					delete_data += [item['img_id']]
					insert_data += [{'img_id' : item['img_id']}]

				buf.delete_many({'img_id' : {'$in' : delete_data}})
				print(delete_data)
				buf.insert_many(insert_data)
				tp = buf.find({})
				for item in tp:
					print(item)
			else:
				result = self.collection.find({'img_id' : {'$in' : ids}, entity : {'$lt' : 100}})

				delete_data = []

				for item in result:
					delete_data += [item['img_id']]

				buf.delete_many({'img_id' : {'$in' : delete_data}})

				tp = buf.find({})
				for item in tp:
					print(item)
		return