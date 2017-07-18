"""
Utils to create lambda functions and apply a mapreduce.
"""
import uuid
import boto3
import json
from multiprocessing.pool import ThreadPool


from utils.packaging import package_with_dependencies
from config import (AWS_ROLE_ARN, VPC_CONFIG, REDIS_HOST, REDIS_PORT, TOPIC_ARN,
                    AWS_REGION, RESULT_QUEUE, SHUFFLER)


lambdacli = boto3.client('lambda', region_name=AWS_REGION)
sqscli = boto3.resource('sqs', region_name=AWS_REGION)


def uuid_str():
    """ Generate an uninque id."""
    return str(uuid.uuid4())


def get_lambda_client():
    """ Just returns a boto3 lambda client connection."""
    return lambdacli


def new_lambdaprivate(name, handler):
    """
    Packages all files that the function *handler* depends on into a zip.
    Creates a new lambda with name *name* on AWS using that zip.
    This lambda will use the VPC_CONFIG, so it is placed and executed inside
    that VPC.
    """
    # zip function-module and dependencies (cloudobj.py, redis, etc.)
    zipfile, lamhand = package_with_dependencies(handler)

    # create the new lambda by uploading the zip. lamhand is the handler
    response = lambdacli.create_function(
        FunctionName=name,
        Runtime='python2.7',
        Role=AWS_ROLE_ARN,
        Handler=lamhand,
        Code={'ZipFile': zipfile.getvalue()},
        Publish=True,
        Description='Lambda with cloud object inside VPC.',
        Timeout=15,
        MemorySize=128,
        VpcConfig=VPC_CONFIG,
        # DeadLetterConfig={
        #     'TargetArn': 'string'
        # },
        Environment={
            'Variables': {
                'REDIS_HOST': REDIS_HOST,
                'REDIS_PORT': str(REDIS_PORT)
            }
        }
        # KMSKeyArn='string',
        # TracingConfig={
        #     'Mode': 'Active'|'PassThrough'
        # },
        # Tags={
        #     'string': 'string'
        # }
    )
    print response
    print "New lambda {} created successfully.".format(name)


def new_lambda(name, handler):
    """
    Packages all files that the function *handler* depends on into a zip.
    Creates a new lambda with name *name* on AWS using that zip.
    This one does not have VPC config. So it has acces to extern services
    such as SNS and SQS. (But can't connect directly to Redis)
    """
    # zip function-module and dependencies
    zipfile, lamhand = package_with_dependencies(handler)

    # create the new lambda by uploading the zip.
    response = lambdacli.create_function(
        FunctionName=name,
        Runtime='python2.7',
        Role=AWS_ROLE_ARN,
        Handler=lamhand,
        Code={'ZipFile': zipfile.getvalue()},
        Publish=True,
        Description='Lambda with cloud object.',
        Timeout=60,
        MemorySize=128,
        # VpcConfig=VPC_CONFIG,
        # DeadLetterConfig={
        #     'TargetArn': 'string'
        # },
        Environment={
            'Variables': { # FIXME: Variables should be requested. Timeout too.
                'RESULT_QUEUE': RESULT_QUEUE,
                'SHUF': SHUFFLER
            }
        }
        # KMSKeyArn='string',
        # TracingConfig={
        #     'Mode': 'Active'|'PassThrough'
        # },
        # Tags={
        #     'string': 'string'
        # }
    )
    print response
    print "New lambda {} created successfully.".format(name)


def delete_lambda(name):
    """ Deletes a lambda functin from AWS with name *name*."""
    response = lambdacli.delete_function(FunctionName=name)
    print response
    print "Lambda {} deleted successfully.".format(name)


class Executor(object):
    """
    Implements map-reduce to be executed on lambda.
    """
    def mapreduce(self, mfunc, rfunc, values, pool_threads=32):
        """
        mfunc: mapper lambda function
        rfunc: reducer lambda function
        values: list of data
        """
        # decide number of mappers on values length
        nmaprs = len(values)
        # print nmaprs

        jobid = uuid_str()

        # invoke the lambda n times to map function to values
        pool = ThreadPool(pool_threads)
        # print values
        def invoke(data):
            payload = {
                'data': data,
                'n': nmaprs,
                'id': jobid,
                'lredu': rfunc
            }
            # print "invoking ", payload
            lambdacli.invoke(FunctionName=mfunc,
                             Payload=json.dumps(payload),
                             InvocationType='Event')
        calls = []
        for i in xrange(nmaprs):
            co = pool.apply_async(invoke, (values[i],))
            calls.append(co)

        res = [c.get() for c in calls]
        pool.close()
        pool.join()

        # wait for end event on the SQS queue.
        queue = sqscli.create_queue(QueueName=RESULT_QUEUE)
        received = False
        while not received:
            for msgs in queue.receive_messages():
                # print msgs
                msg = json.loads(msgs.body)
                if (msg['id']) == jobid:
                    received = True
                    results = msg['results']
                    msgs.delete()
                else:
                    print "ID ERROR"

        return results
