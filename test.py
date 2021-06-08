from pymongo import MongoClient
from feedback import Feedback
client = MongoClient('localhost', 27017)
pic = client['pic']
collection1 = pic['collection1']
a = ['4']



model = Feedback(client, 'pic', 'collection1')

#model.feedback(['cat'], [2, True)
model.feedback(['cat', 'dog'], a, True)
print(model.multi_tag_query(['cat', 'dog']))
print(model.feedback_folder(['cat'], './data', True))
