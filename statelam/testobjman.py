""" Test for the LambObj class.
Change the *resource* definition in lambobjman handler to test it ofline.
"""
import unittest
import json
import lambobjman


def invoke(event):
    """ Fakes a lambda invocation to the lambobjman handler."""
    return lambobjman.object_handler(event, None)


class TestObjMan(unittest.TestCase):
    def setUp(self):
        self.lambname = 'testinglambda'

    def tearDown(self):
        pass

    def test_1_new_object(self):    # set / get
        # obj.myatt = 'value'
        att_name = 'myatt'
        event = {
            'function': self.lambname,
            'type': 'attr',
            'attr': att_name,
            'args': {
                'meth': 'set',
                'value': 'value'
                }
        }
        response = invoke(event)

        # 'print' obj.myatt
        event = {
            'function': self.lambname,
            'type': 'attr',
            'attr': att_name,
            'args': {'meth': 'get'}
        }
        response = invoke(event)
        self.assertEquals(response, 'value')

        # obj.clear()
        event = {
            'function': self.lambname,
            'type': 'clear',
            'attr': 'all',
            'args': {}
        }
        response = invoke(event)

    def test_1_new_objectint(self):    # set / get
        # obj.myatt = 1
        att_name = 'myatt'
        event = {
            'function': self.lambname,
            'type': 'attr',
            'attr': att_name,
            'args': {
                'meth': 'set',
                'value': 1
                }
        }
        response = invoke(event)

        # 'print' obj.myatt
        event = {
            'function': self.lambname,
            'type': 'attr',
            'attr': att_name,
            'args': {'meth': 'get'}
        }
        response = invoke(event)
        self.assertEquals(response, 1)

        # obj.clear()
        event = {
            'function': self.lambname,
            'type': 'clear',
            'attr': 'all',
            'args': {}
        }
        response = invoke(event)

    def test_2_lists(self):
        list_name = 'somelist'
        # somelist = obj.list('somelist')
        # somelist.append(1)
        event = {
            'function': self.lambname,
            'type': 'list',
            'attr': list_name,
            'args': {
                'meth': 'appnd',
                # 'indx': 'appnd',
                'value': 'helllllo'
                }
        }
        response = invoke(event)

        # somelist[0]  == 1 ??
        event = {
            'function': self.lambname,
            'type': 'list',
            'attr': list_name,
            'args': {
                'meth': 'get',
                'indx': 0
                }
        }
        response = invoke(event)
        self.assertEquals(response, 'helllllo')

        # somelist.clear()
        event = {
            'function': self.lambname,
            'type': 'clear',
            'attr': list_name,
            'args': {}
        }
        response = invoke(event)


if __name__ == '__main__':
    # print ('## Run the tests.')
    suite = unittest.TestLoader().loadTestsFromTestCase(TestObjMan)
    unittest.TextTestRunner(verbosity=2).run(suite)
