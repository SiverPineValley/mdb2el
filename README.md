# mdb2el
MongoDB to Elasticsearch Data Sending Tool <br>

## How to use
본 오픈소스는 Python3 버젼을 지원하며, 3.6버젼에서 작업했습니다. Pymongo, elasticsearch_py, tqdm의 파이썬 라이브러리들을 필요로 합니다. Python 코드로 실행하기 위해서는 python run.py를 통해 실행할 수 있으며, C코드를 이용하여 configuration file을 이용할 때는, make을 통해 빌드하고, mdb2el을 실행시켜서 사용할 수 있습니다. 하지만, 사용하기 전에 run.py 혹은 mdb2el.c의 코드를 수정하여 파라미터를 변경해줄 필요가 있습니다.

## OpenSource SW Licenses
inih: New BSD license
https://github.com/benhoyt/inih/blob/master/LICENSE.txt

pymongo: Apache License Version 2.0
https://github.com/mongodb/mongo-python-driver/blob/master/LICENSE

elasticsearch: Apache License Version 2.0
https://elasticsearch-py.readthedocs.io/en/master/

tqdm: MIT licence
https://github.com/tqdm/tqdm/blob/master/LICENCE
