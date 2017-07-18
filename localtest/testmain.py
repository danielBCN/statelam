""" Trying CloudObj with a local Redis."""
import cloudobj


if __name__ == '__main__':
    # Try things
    resource = {'host': '192.168.99.100', 'port':6379}
    obj = cloudobj.CloudObj('att1', resource)

    print obj.id

    # obj.value = int(getattr(obj, 'value', 0)) + 1

    # obj.value = 1

    # print obj.value

    cllist = obj.list('cllist')

    cllist.append(1)
    cllist[0] = 3

    print cllist[0]
    cllist.append(5)
    print cllist

    cllist.clear()
    obj.clear()
