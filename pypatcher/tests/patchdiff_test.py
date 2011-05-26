import shutil
import unittest
import tempfile

from .. import patchdiff

class TestSimple(unittest.TestCase):

    def setUp(self):
        self.wd = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.wd)

    def testMakeNewFile(self):
        """
        Tests if given a new file, it is copied across
        in the patching process
        """
        orig = os.path.join(self.wd, 'orig')
        origF = os.path.join(new, 'new.file')

        new = os.path.join(self.wd, 'new')
        newF = os.path.join(new, 'new.file')

        patchF = os.path.join(self.wd, 'patch.file')
        temp = os.path.join(self.wd, 'temp')

        os.makedirs(orig)
        os.makedirs(new)
        with open(newF) as f:
            f.write('some text')

        patchdiff.generateDiff(orig, new, patchF)
        patchdiff.mergePatches(orig, [patchF], temp)
        patchdiff.applyPatchDirectory(orig, temp)

        self.assertTrue(os.path.exists(origF))


if __name__ == '__main__':
    unittest.main()
