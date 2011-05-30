import unittest
import shutil
import tempfile

from .. import partialdl

class TestSimple(unittest.TestCase):

    def setUp(self):
        self.wd = tempfile.mkdtemp()
        self.pd = partialdl.PartialDownloader(self.wd)

    def tearDown(self):
        shutil.rmtree(self.wd)

    def testAdd(self):
        self.pd.add('http://www.example.com/file', 'fn')
        self.assertTrue(self.pd.hasUrl('http://www.example.com/file'))
        self.assertTrue(self.pd.hasDst('fn'))

    def testLock(self):
        self.assertRaises(partialdl.LockError,
                          partialdl.PartialDownloader,
                          self.wd)
        

if __name__ == '__main__':
    unittest.main()
