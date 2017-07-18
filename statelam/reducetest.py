"""
Lambda definition. Applies a reduce to a list of values with the same key.
If it is the last reducer, return the result to client.
Implements wordcount. (an add)
"""
import os
import lambobj
import boto3
import json


def reducer_handler(event, context):
    obj = lambobj.LambObj(context.function_name)

    results = obj.list('results')
    if (event.has_key('clear')):    # Debugging
        results.clear()
        obj.clear()
        return
    nk = int(event['n'])
    data = event['data']
    jobid = event['id']

    # Apply the reduce function to the word count
    for key in data:
        fr = lambda v1, v2: (v1[0], v1[1] + v2[1])
        result = reduce(fr, key)

        print result
        results.append(result)
        nr = obj.incr(jobid)
        print nr

        # check if all data has been reduced
        # If so, generate an event that returns the entire list of results
        if nr == nk:
            res = []
            for i in xrange(nk):
                res.append(results[i])
            results.clear()
            obj.delete(jobid)
            print res
            result_queuen = os.environ['RESULT_QUEUE']
            sqscli = boto3.client('sqs')
            queue = sqscli.create_queue(QueueName=result_queuen)['QueueUrl']
            print queue
            msg = {'id': jobid, 'results': res}
            response = sqscli.send_message(QueueUrl=queue, MessageBody=json.dumps(msg))
