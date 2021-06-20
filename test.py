from pymongo import MongoClient
from feedback import Feedback

client = MongoClient('localhost', 27017)
pic = client['pic']
collection1 = pic['collection1']
model = Feedback(client, 'pic', 'collection1')








#for i in range(200):
#	model.feedback(['cat'], ['60c30f03feb9266f9c34bdbe'], False)





for i in range(200):
	model.feedback_folder('cat', './data/cat', True)

result = model.single_tag_query('cat')
for item in result:
	print(item)

model.feedback_all_folder('./data', True)

