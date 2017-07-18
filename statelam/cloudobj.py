"""
An object which attributes are stored on an external service (Redis in this case)
that also allows creating lists.
"""
import redis
import pickle


class CloudObj(object):
    """ This special object. """
    def __init__(self, oid, res):
        self.id = oid
        self.r =  redis.Redis(host=res['host'], port=res['port'], db=0)
        self.prefix = res['prefix'] if 'prefix' in res.keys() else 'cloudobjs'
        # print self.r.incr('id')

    def __getattribute__(self, name):
        """
        Getting an attribute of this object means accessing the external
        resource.
        """
        if name in ['r', 'id', 'prefix', 'clear', 'list', 'get', 'incr', 'delete']:
            return object.__getattribute__(self, name)
        else:
            value = self.r.hget(self.prefix + ':' + self.id, name)
            if value is not None:
                return pickle.loads(value)
            else:
                raise AttributeError("'CloudObj' has no attribute '" +
                                     name + "'")

    def get(self, name):
        """
        Needed to dynamically get attributes correctly.
        """
        value = self.r.hget(self.prefix + ':' + self.id, name)
        if value is not None:
            return pickle.loads(value)
        else:
            raise AttributeError("'CloudObj' has no attribute '" +
                                 name + "'")

    def __setattr__(self, name, value):
        """
        Setting an attribute of this object means accessing the external
        resource.
        """
        if name in ['r', 'id', 'prefix']:
            return object.__setattr__(self, name, value)
        else:
            ser = pickle.dumps(value)
            self.r.hset(self.prefix + ':' + self.id, name, ser)

    def clear(self):
        """
        Delete all keys on Redis that references this object.
        """
        # keys = self.r.keys(self.id + '*')
        # for k in keys:
        self.r.delete(self.prefix + ':' + self.id)

    def list(self, name):
        """ Create a list object relative to this object."""
        return CloudList(self.id, self.r, self.prefix, name)

    def incr(self, name):
        """ Basic counter. """
        return self.r.incr(self.prefix + ':' + self.id + ':' + name)

    def delete(self, name):
        """ Allows deletion of counters. """
        self.r.delete(self.prefix + ':' + self.id + ':' + name)

    # def __del__(self):
    #     self.r.delete(self.prefix + ':' + self.id)


class CloudList(object):
    """ A list that exists on Redis."""
    def __init__(self, oid, r, prefix, name):
        self.id = oid
        self.r =  r
        self.prefix = prefix
        self.name = name
        self.key = self.prefix + ':' + self.id + ':' + name

    def append(self, value):
        """ Appends goes to redis. """
        self.r.rpush(self.key, pickle.dumps(value))

    def clear(self):
        """ Delete the redis key for the list. """
        self.r.delete(self.key)

    def __getitem__(self, index):
        """ Get an item from the list as list[ind] """
        return pickle.loads(self.r.lindex(self.key, index))

    def __setitem__(self, index, value):
        """ Set an item on the list as list[ind] = value """
        self.r.lset(self.key, index, pickle.dumps(value))

    def __len__(self):
        """ Get the lenght of the list."""
        return self.r.llen(self.key)
