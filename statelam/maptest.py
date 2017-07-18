"""
Lambda definition. Applies a map and stores the parcial result on a shared object.
If it is the last mapper, trigger the next step, the shuffler.
Implements wordcount.
"""
import os
import lambobj
import boto3
import json


def mapper_handler(event, context):
    obj = lambobj.LambObj(context.function_name)

    results = obj.list('results')
    if (event.has_key('clear')):  # Debugging. Clear keys.
        results.clear()
        obj.clear()
        return
    nmappers = int(event['n'])
    line = event['data']
    jobid = event['id']

    # Apply function and store the parcial result on a redis list

    line = line.lower()
    line = line.replace('.', '').replace(',', '').replace(':', '')

    words = line.split()
    counts = {}
    for word in words:
        counts[word] = counts.get(word, 0) + 1

    result = [(word, counts[word]) for word in counts.keys()]

    print result
    results.append(result)
    nm = obj.incr(jobid)
    print nm

    # check if all data has been mapped
    # If so, generate an event that passes the entire list of results
    if nm == nmappers:
        res = []
        for i in xrange(nmappers):
            res.append(results[i])
        results.clear()
        obj.delete(jobid)
        print res

        lambdacli = boto3.client('lambda')
        # Send results to the shuffler to reduce
        shuffler = os.environ['SHUF']
        payload = {
            'data': res,
            'id': jobid,
            'redulamb': event['lredu']
        }
        lambdacli.invoke(FunctionName=shuffler,
                         Payload=json.dumps(payload),
                         InvocationType='Event')
