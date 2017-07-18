"""
Lambda definition. Receives a message from the LambObj and executes the same
on a CloudObj.
"""
import os
import cloudobj
import boto3
import json


def object_handler(event, context):
    """ The lambda handler """
    resource = {'host': os.environ['REDIS_HOST'],
                'port':int(os.environ['REDIS_PORT'])}
    # resource = {'host': '192.168.99.100', 'port':6379}

    func_name = event['function']

    obj = cloudobj.CloudObj(func_name, resource)

    typ = event['type']
    att = event['attr']
    args = event['args']

    if typ == 'attr':
        return manage_attribute(obj, att, **args)
    elif typ == 'clear':
        return manage_clear(obj, att)
    elif typ == 'list':
        return manage_list(obj, att, **args)
    elif typ == 'incr':
        return obj.incr(att)
    elif typ == 'del':
        return obj.delete(att)
    else:
        raise Exception('Bad type event.')

def manage_attribute(cobj, att_name, meth, value=None):
    if meth == 'set':
        setattr(cobj, att_name, value)
    elif meth == 'get':
        return cobj.get(att_name)
    else:
        raise Exception('No such access method: ', meth)

def manage_clear(cobj, mode):
    if mode == 'all':
        cobj.clear()
    else:
        l = cobj.list(mode)
        l.clear()

def manage_list(cobj, lname, meth, indx=None, value=None):
    l = cobj.list(lname)
    if meth == 'appnd':
        l.append(value)
    elif meth == 'get':
        return l[indx]
    elif meth == 'set':
        l[indx] = value
    elif meth == 'len':
        return len(l)
    else:
        raise Exception('No such access method: ', meth)
