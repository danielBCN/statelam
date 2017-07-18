""" Main test script
"""
import statelam.maptest
import statelam.reducetest
import statelam.sortnshuffle
import statelam.lambobjman
import statelam.client
from itertools import groupby
from operator import itemgetter
import pickle


lamcli = statelam.client.get_lambda_client()

# Deploy lambdas needed, only one time.
# client.new_lambda('mappertest', statelam.maptest.mapper_handler)
# client.new_lambda('reducertest', statelam.reducetest.reducer_handler)
# client.new_lambda('sortnshuffle', statelam.sortnshuffle.sorter_handler)

# client.new_lambdaprivate('redisacces', lambobjman.object_handler)

exe = statelam.client.Executor()

with open('basicwordcount/file.txt') as f:
    lines = f.readlines()

result = exe.mapreduce('mappertest', 'reducertest', lines)

print result
