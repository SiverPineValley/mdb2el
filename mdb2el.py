from pymongo import MongoClient
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from tqdm import tqdm
import datetime

class mdb2el:
	"""
	mdb2el은 mongoDB의 Data를 query에 따라 data를 찾은 후 해당 조건의 data들을

	원하는 Elasticsearch DB에 올릴 수 있는 Library이다.

	_Bulk API, Epoch 추가 버젼

	사용법: 
	import mdb2el

	# 쿼리 dictionary 설정
	query = {}

	# mdb2el Object 생성
	test = mdb2el.mdb2el( dbname= 'test', colname= 'test', elidx= 'test', eltype= 'test', findquery= query )
	
	# 실행
	test.run()

	"""

	def __init__(self, dbname, colname, elidx, eltype, findquery = {},
		dbhost = 'localhost', dbport = 27017, elhost = 'localhost', elport = 9200, ep = 100 ):
		"""
		mdb2el의 Constructor.
		
		Arguments:
			dbname {string} -- MongoDB의 DB name
			colname {string} -- MongoDB의 Collection name
			elidx {string} -- Elasticsearch의 Index name
			eltype {string} -- Elasticsearch의 Index type name
		
		Keyword Arguments:
			dbhost {str} -- MongoDB ip host 						(default: {'localhost'})
			dbport {number} -- MongoDB port number 					(default: {27017})
			elhost {str} -- Elasticsearch ip port 					(default: {'localhost'})
			elport {number} -- Elasticsearch port number 			(default: {9200})
			findquery {dictionary} -- MongoDB에서 원하는 Data를 찾기 위한 query dictionary (default: {})
			ep {number} -- 한 번에 받아올 Document 개수 (default: {5000})

		Instance:
			client {MongoClient Object} -- Object내에서 사용되는 MongoDB와의 연결 Client Object
			db {database Object} -- Object내에서 사용되는 MongoDB의 DB
			col {collection Object} -- Object내에서 사용되는 MongoDB의 Collection
			es {Elasticsearch Object} -- Object내에서 사용되는 Elasticsearch와의 연결을 담당하는 Object
			idx {string} -- Object내에서 사용되는 Elasticsearch Index name
			dtype {string} -- Object내에서 사용되는 Elasticsearch Type name
			dlist {dictionary list} -- MongoDB에서 받아온 Document들을 dictionary 형식으로 보관하고 있는 list
			did {string} -- MongoDB에서 받아온 Document들이 가진 ObjectID형식의 Data를 String으로 보관하고 있는 list
			findquery {dictionary} -- Object내에서 사용되는 MongoDB에서 원하는 Data를 찾기 위한 query dictionary
			ndoc {int} -- MongoDB에서 받아온 Document들의 수
		"""

		# MongoDB ip주소와, port에 따라 client와 연결하고, DB name과 Collection name에 따라 DB를 불러온다.
		self.client = MongoClient(dbhost, dbport)

		self.db = self.client.get_database(name=dbname)
		self.col = self.db.get_collection(name=colname)

		# host ip와 port를 통해 elasticsearch의 node에 연결한다.
		self.es = Elasticsearch([{'host': elhost, 'port': elport}])

		self.idx = elidx
		self.dtype = eltype
		
		self.dlist = []
		self.did = []
		self.findquery = findquery
		self.ndoc = self.col.find(self.findquery).count()
		self.ep = ep

	def run(self):
		"""
		이 메소드를 통해 Class 내부의 instance를 이용해 data를 MongoDB에서 가져와서

		Elasticsearch DB에 올리는 작업을 연속으로 수행한다.
		"""
		# 매핑 정보가 있는지 확인한다.

		print("----- State: Get " + self.col.name + " Documents from MongoDB and Sending to Elasticsearch -----\n")
		self._get_data_from_mongo_and_send()

		print("----- State: Sending Finished! -----\n\n\n")

		# ndoc, dlist, did 변수들을 초기화해준다.
		self.ndoc = 0
		self.dlist = []
		self.did = []

	def _get_data_from_mongo_and_send(self):
		"""
		이 메소드는 MongoDB에서 query에 따른 Document들을 찾아서 Class 내부의 dlist instance에

		추가하는 작업을 수행한다. 각 Data는 JSON type으로 변형된다.
		"""
		for epoch in tqdm(range(int(self.ndoc/self.ep)+1)):
			for i, doc in enumerate(list( self.col.find(self.findquery).skip(epoch*self.ep).limit(self.ep) )):

				# _id field 값을 별도로 저장한다.
			    self.did.append(str(doc["_id"]))

			    # 원본 Document에서는 _id field를 제거한다.
			    del doc["_id"]

			    # Bulk Api를 사용하기 위해 Bulk Header를 붙여준다.
			    doc = { "_index": self.idx, "_type": self.dtype, "_id": self.did[i], "_source": doc }

			    # 하나의 Document 양식을 만든 후 dlist에 추가
			    self.dlist.append(doc)

			self._put_data_to_el()
			
			# Epoch 초기화
			self.did = []
			self.dlist = []

	def _put_data_to_el(self):
		"""
		이 메소드는 MongoDB에서 받아온 Data를 Elasticsearch에 넣는 작업을 수행한다.
		"""
		helpers.bulk(self.es, self.dlist)

	def set_config(self, dbname, colname, elidx, eltype, findquery,
		dbhost = 'localhost', dbport = 27017, elhost = 'localhost', elport = 9200, ep = 100 ):
		"""
		이 함수는 Object의 내부 정보를 초기화하는 역할을 해준다. 구조체의 역할을 해준다.
		
		Arguments:
			dbname {string} -- MongoDB의 DB name
			colname {string} -- MongoDB의 Collection name
			elidx {string} -- Elasticsearch의 Index name
			eltype {string} -- Elasticsearch의 Index type name
			findquery {dictionary} -- MongoDB에서 원하는 Data를 찾기 위한 query dictionary
		
		Keyword Arguments:
			dbhost {str} -- MongoDB ip host 						(default: {'localhost'})
			dbport {number} -- MongoDB port number 					(default: {27017})
			elhost {str} -- Elasticsearch ip port 					(default: {'localhost'})
			elport {number} -- Elasticsearch port number 			(default: {9200})
		"""
		self.client = MongoClient(dbhost, dbport)

		self.db = self.client.get_database(name=dbname)
		self.col = self.db.get_collection(name=colname)

		# host ip와 port를 통해 elasticsearch의 node에 연결한다.
		self.es = Elasticsearch([{'host': elhost, 'port': elport}])

		self.idx = elidx
		self.dtype = eltype

		self.findquery = findquery
		self.ndoc = self.col.find(self.findquery).count()
		self.ep = ep