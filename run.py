import mdb2el as me

# 쿼리 dictionary 설정

query = {}


# dbname, collection name, index, type, query를 parameter로
# 추가적으로 dbhost = 'localhost', dbport = 27017, elhost = 'localhost', elport = 9200 파라미터로 입력 가능

me.mdb2el( dbname = 'test', colname = 'test', elidx = 'test', eltype = 'test', findquery = query ).run()