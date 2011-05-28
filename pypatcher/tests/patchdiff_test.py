import os
import shutil
import unittest
import tempfile
import filecmp


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
        new = os.path.join(self.wd, 'new')

        newF = os.path.join(new, 'new.file')
        origF = os.path.join(orig, 'new.file')

        patchF = os.path.join(self.wd, 'patch.file')
        temp = os.path.join(self.wd, 'temp')

        os.makedirs(orig)
        os.makedirs(new)
        with open(newF, 'w') as f:
            f.write('some text')

        patchdiff.generateDiff(orig, new, patchF)
        self.assertTrue( os.path.isfile(patchF) )
        patchdiff.mergePatches(orig, temp, [patchF])
        patchdiff.applyPatchDirectory(orig, temp)

        self.assertTrue(os.path.exists(origF))

    def testPatchText(self):
        """
        Tests to see if a text patch is correctly applied
        """
        orig = os.path.join(self.wd, 'orig')
        new = os.path.join(self.wd, 'new')

        newF = os.path.join(new, 'patched.file')
        origF = os.path.join(orig, 'patched.file')

        patchF = os.path.join(self.wd, 'patch.file')
        temp = os.path.join(self.wd, 'temp')

        os.makedirs(orig)
        os.makedirs(new)
        with open(origF, 'w') as f:
            f.write('some text')

        with open(newF, 'w') as f:
            f.write('some more text')

        patchdiff.generateDiff(orig, new, patchF)
        self.assertTrue( os.path.isfile(patchF) )
        patchdiff.mergePatches(orig, temp, [patchF])
        patchdiff.applyPatchDirectory(orig, temp)

        self.assertTrue(os.path.exists(origF))
        self.assertTrue(os.path.exists(newF))

        self.assertTrue(filecmp.cmp(origF, newF, False))

    def testPatchBin(self):
        """
        Tests to see if a binary patch is correctly applied
        """
        orig = os.path.join(self.wd, 'orig')
        new = os.path.join(self.wd, 'new')

        newF = os.path.join(new, 'patched.file')
        origF = os.path.join(orig, 'patched.file')

        patchF = os.path.join(self.wd, 'patch.file')
        temp = os.path.join(self.wd, 'temp')

        os.makedirs(orig)
        os.makedirs(new)
        with open(origF, 'w') as f:
            f.write('\0This is \0a binary file')
        self.assertFalse(patchdiff._isText(origF))

        with open(newF, 'w') as f:
            f.write('\0This is \0a newer binary file')

        patchdiff.generateDiff(orig, new, patchF)
        self.assertTrue( os.path.isfile(patchF) )
        patchdiff.mergePatches(orig, temp, [patchF])
        patchdiff.applyPatchDirectory(orig, temp)

        self.assertTrue(os.path.exists(origF))
        self.assertTrue(os.path.exists(newF))

        self.assertTrue(filecmp.cmp(origF, newF, False))

    def testMultiple(self):
        """
        Tests to see if we can do multiple patches
        """
        NUM_PATCHES = 2

        dirs = []
        for i in range(NUM_PATCHES+1):
            d = os.path.join(self.wd, str(i))
            dirs.append(d)
            os.makedirs(d)
        
        files = []
        for i in range(NUM_PATCHES+1):
            files.append(os.path.join(dirs[i], 'patched.file'))

        patches = []
        for i in range(NUM_PATCHES):
            patches.append(os.path.join(self.wd, 'patch.' + str(i)))

        temp = os.path.join(self.wd, 'temp')

        fileText = 'This is some text'
        for i in range(NUM_PATCHES+1):
            with open(files[i], 'w') as f:
                f.write(fileText + str(i))

        for i in range(NUM_PATCHES):
            patchdiff.generateDiff(dirs[i], dirs[i+1], patches[i])
            self.assertTrue( os.path.isfile(patches[i]) )

        patchdiff.mergePatches(dirs[0], temp, patches)
        patchdiff.applyPatchDirectory(dirs[0], temp)

        self.assertTrue(filecmp.cmp(files[0], files[NUM_PATCHES], False))

if __name__ == '__main__':
    unittest.main()
