"""
As wit cloudobj, but this objects translate the accesses to lambda invocations.
This way we can access Redis (inside a VPC) and use the data outside.
"""
import pickle
import boto3
import json


AWS_REGION = 'us-west-2'
lambdacli = boto3.client('lambda', region_name=AWS_REGION)


class LambObj(object):
    def __init__(self, oid):
        self.id = oid

    def __getattribute__(self, name):
        if name in ['id', 'clear', 'list', 'get', 'incr', 'delete']:
            return object.__getattribute__(self, name)
        else:
            event = {
                'function': self.id,
                'type': 'attr',
                'attr': name,
                'args': {
                    'meth': 'get'
                    }
            }
            resp = lambdacli.invoke(
                FunctionName='redisacces',
                Payload=json.dumps(event)
              )
            value = pickle.loads(json.loads(resp['Payload'].read()))
            if value is not None:
                return value
            else:
                raise AttributeError("'CloudObj' has no attribute '" +
                                     name + "'")

    def __setattr__(self, name, value):
        if name in ['id']:
            return object.__setattr__(self, name, value)
        else:
            # ser = pickle.dumps(value)
            event = {
                'function': self.id,
                'type': 'attr',
                'attr': name,
                'args': {
                    'meth': 'set',
                    'value': pickle.dumps(value)
                    }
            }
            resp = lambdacli.invoke(
                FunctionName='redisacces',
                Payload=json.dumps(event)
              )
            # self.r.hset(self.prefix + ':' + self.id, name, ser)

    def clear(self):
        event = {
            'function': self.id,
            'type': 'clear',
            'attr': 'all',
            'args': {}
        }
        resp = lambdacli.invoke(
            FunctionName='redisacces',
            Payload=json.dumps(event)
          )

    def list(self, name):
        return LambList(self.id, name)

    def incr(self, name):
        event = {
            'function': self.id,
            'type': 'incr',
            'attr': name,
            'args': {}
        }
        resp = lambdacli.invoke(
            FunctionName='redisacces',
            Payload=json.dumps(event)
          )
        value = json.loads(resp['Payload'].read())
        return value

    def delete(self, name):
        event = {
            'function': self.id,
            'type': 'del',
            'attr': name,
            'args': {}
        }
        resp = lambdacli.invoke(
            FunctionName='redisacces',
            Payload=json.dumps(event)
          )

    # def __del__(self):
    #     self.r.delete(self.prefix + ':' + self.id)


class LambList(object):
    def __init__(self, oid, name):
        self.id = oid
        self.name = name

    def append(self, value):
        # self.r.rpush(self.key, pickle.dumps(value))
        event = {
            'function': self.id,
            'type': 'list',
            'attr': self.name,
            'args': {
                'meth': 'appnd',
                # 'indx': 'appnd',
                'value': pickle.dumps(value)
                }
        }
        resp = lambdacli.invoke(
            FunctionName='redisacces',
            Payload=json.dumps(event)
          )

    def clear(self):
        # self.r.delete(self.key)
        event = {
            'function': self.id,
            'type': 'clear',
            'attr': self.name,
            'args': {}
        }
        resp = lambdacli.invoke(
            FunctionName='redisacces',
            Payload=json.dumps(event)
          )

    def __getitem__(self, index):
        # return pickle.loads(self.r.lindex(self.key, index))
        event = {
            'function': self.id,
            'type': 'list',
            'attr': self.name,
            'args': {
                'meth': 'get',
                'indx': index
                }
        }
        resp = lambdacli.invoke(
            FunctionName='redisacces',
            Payload=json.dumps(event)
        )
        value = json.loads(resp['Payload'].read())
        return pickle.loads(value)

    def __setitem__(self, index, value):
        # self.r.lset(self.key, index, pickle.dumps(value))
        event = {
            'function': self.id,
            'type': 'list',
            'attr': self.name,
            'args': {
                'meth': 'set',
                'indx': index,
                'value': pickle.dumps(value)
                }
        }
        resp = lambdacli.invoke(
            FunctionName='redisacces',
            Payload=json.dumps(event)
        )

    def __len__(self):
        # return self.r.llen(self.key)
        event = {
            'function': self.id,
            'type': 'list',
            'attr': self.name,
            'args': {
                'meth': 'len'
                }
        }
        resp = lambdacli.invoke(
            FunctionName='redisacces',
            Payload=json.dumps(event)
        )
        value = json.loads(resp['Payload'].read())
        return value
