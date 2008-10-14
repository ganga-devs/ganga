import unittest

class GangaGPITestCase(unittest.TestCase):
	"""
	Base class for GPI test-cases
	"""
	def __init__(self, methodName='runTest'):
		unittest.TestCase.__init__(self,methodName)


	def setUp(self):
		pass

	def tearDown(self):
		pass

	def runTest():
		pass

from pytf.lib import MultipassTest as MP 
class MultipassTest(MP):
	"""
	Base class for multi-pass tests
	"""
	def setUp(self):
		pass

	def tearDown(self):
		pass


