""" Main test script
"""
import maptest
import reducetest
import sortnshuffle
import lambobjman
import client
from itertools import groupby
from operator import itemgetter
import pickle


lamcli = client.get_lambda_client()

# Deploy lambdas needed, only one time.
# client.new_lambda('mappertest', maptest.mapper_handler)
# client.new_lambda('reducertest', reducetest.reducer_handler)
# client.new_lambda('sortnshuffle', sortnshuffle.sorter_handler)

# client.new_lambdaprivate('redisacces', lambobjman.object_handler)

exe = client.Executor()

with open('../basicwordcount/file.txt') as f:
    lines = f.readlines()

result = exe.mapreduce('mappertest', 'reducertest', lines, nr=32)
result.sort()
print result
