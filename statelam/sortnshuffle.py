"""
Lambda definition. Sorts the intermediate results of the map and distributes
them among reducers.
"""
import os
import boto3
import json

from itertools import groupby
from operator import itemgetter


def sorter_handler(event, context):
    lambdacli = boto3.client('lambda')
    mapresul = event['data']
    jobid = event['id']
    redulamb = event['redulamb']

    # flat the list
    allkeys = [tup for res in mapresul for tup in res]

    allkeys.sort()   # sort the key-value so can be grouped
    groups = groupby(allkeys, itemgetter(0))

    # distribute the groups in a list of lists that contain key-value pairs for each key
    # each group is a key
    x = [[(str(key), value) for key, value in group] for current_key, group in groups]
    print x

    # invoke reducers
    nk = len(x)
    nr = event['nr'] if event.has_key('nr') else nk
    for i in xrange(nr):
        w = []
        q = i
        while q < nk:
            w.append(x[q])
            q += nr
        payload = {
            'data': w,
            'n': nk,
            'id': jobid
        }
        lambdacli.invoke(FunctionName=redulamb,
                         Payload=json.dumps(payload),
                         InvocationType='Event')
        print i
