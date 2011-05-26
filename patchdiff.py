"""
This module allows the diff and patching of directories.

It is a wrapper around bsdiff and bspatch, for lack of a better
method of doing it in python. This means that it is expcected that
bsdiff.exe and bspath.exe are on the path
"""

import os
import sys
import xmlrpclib
import glob
import threading
import tempfile
import zipfile
import json
import hashlib

from partialdl import PartialDownloader

#the binary patching/dif files
BSDIFF = 'bsdiff.exe'
BSPATCH = 'bspatch.exe'

#the extension for the downloaded patch files
PATCH_EXT = 'cpatch'

#the name of the config file that holds details about the patch
PATCH_CFG = 'cfg.json'

#dir that holds the actual patch files
PATCH_DIR = 'patchfs'

def getFileMd5(filePath):
    h = hashlib.md5.new()
    with open(filePath) as f:
        h.update(f.read())
    return h.hexdigest()

class PatchError(Exception):
    """
    A generic error occuring upon patching a file
    """
    pass


class Patcher:
    """
    This class takes a set of patch files from a directory, and applies them
    to a src directory.

    It is expected that the patch files has simple integer names that corispond
    to version numbers as that is the method used to tell in which order to apply
    the patches
    """

    def hasUpdate(self, updateDir):
        """
        This checks if updates exist in the dir dest
        """
        return len(glob.glob(os.path.join(updateDir, '*.' + PATCH_EXT))) > 0

    def patch(self, srcDir, outDir, updatesDir):
        """
        This runs patches that have been found. This is run in order
        so the lowest patches are run first, then the next ones etal
        """
        files = glob.glob(os.path.join(updatesDir, '*.' + PATCH_EXT))
        #sort the files so that we patch in the right order
        files.sort(key=lambda x:int(os.path.splitext(os.path.basename(x))))
        for f in files:
            tmpDir = tempfile.mkdtemp() 

            self.extract(f, tmpDir)
            self.applyPatch(srcDir, outDir, tmpDir)

            shutil.rmtree(tmpDir)

    def extract(self, inputFile, destDir):
        with zipfile.ZipFile(inputFile) as zf:
            zf.extractall(destDir)

    def applyPatch(self, srcDir, outDir, patchDir):
        """
        Given two directories, this applies the patch to srcdir from patchdir
        If the file to be patched already exists in ouputDir, then the patch
        will be run on the file in outputDir.
        """
        with open(os.path.join(patchDir, PATCH_CFG)) as cfgFile:
            cfg = json.loads(cfgFile.read())

        #apply all patch files
        patchFilesDir = os.path.join(patchDir, PATCH_DIR)
        for root, dirs, files in os.walk(patchFilesDir):
            #root contains patchdir, which we don't want
            for f in files:
                absFn = os.path.join(root, f) 
                fn = absFn[len(patchFilesDir)+len(os.sep):]
                srcAbsFn = os.path.join(srcDir, fn)
                outAbsFn = os.path.join(outDir, fn)

                filecfg = cfg[fn]

                if os.path.exists(outAbsFn):
                    toPatchAbsFn = outAbsFn
                else:
                    toPatchAbsFn = srcAbsFn
                    
                if os.path.exists(toPatchAbsFn):
                    if getFileMd5(toPatchAbsFn) != filecfg['oldmd5']:
                        raise PatchError('The old patch file wasn\' correct')

                if filecfg['type'] == 'bin':
                    func = self.patchBinFile
                else:
                    func = self.patchFile
                func(toPatchAbsFn, absFn, outAbsFn)

                if getFileMd5(outAbsFn) != filecfg['patchedmd5']:
                    raise PatchError('There was an error patching the file')
        
        #this needs sorting due to the change of method.
        #we shouldn't delete this here we should merge
        #it so that it with other applied patches and
        #then do it in one go when we "run" the patch
        for d in cfg['deleted']:
            delPath = os.path.join(srcDir, d)
            #TODO

    def patchBinFile(self, src, out, patch):
        e = os.spawnl(os.P_WAIT, BSPATCH, src, out, patch)
        
    def patchFile(self, src, out, patch):
        self.patchBinFile(src, out, patch)


class DiffError(Exception):
    pass


class Diff:
    """
    This class generates a compressed patch file from two directories
    """
    def genDir(self, oldDir, newDir, outputFile):
        assert os.path.isdir(oldDir)
        assert os.path.isdir(newDir)

        cfg = {
            'deleted' : [],
        }
        tmpDir = tempfile.mkdtemp() 
        for root, dirs, files in os.walk(newDir):
            for f in files:
                absfn = os.path.join(root, f)
                fn = absfn[len(newDir) + len(os.sep):]
                oldfn = os.path.join(oldDir, fn)

                filecfg = cfg[fn] = {}

                filecfg['oldmd5'] = getFileMd5(oldfn)
                filecfg['patchedmd5'] = getFileMd5(absfn)

                filecfg['types'] = 'other'
                fn = self.genFile
                if fn.endswith('.exe'):
                    filecfg['types'] = 'bin'
                    fn = self.genBinFile

                fn(os.path.join(oldDir, fn),
                   os.path.join(newDir, fn),
                   os.path.join(tmpDir, PATCH_DIR , fn))
        
        cfgOut = os.path.join(tmpDir, PATCH_CFG)
        open(cfgOut).write(json.dumps(cfg)).close()
        self.zipDir(tmpDir, outputFile)

        shutil.rmtree(tmpDir)

    def genFile(self, old, new, patch):
        o = open(old)
        n = open(new)
        p = open(patch, 'w')
        p.writelines(difflib.ndiff(o.readlines(),
                                   n.readlines()))
        o.close()
        n.close()
        p.close()

    def genBinFile(self, old, new, patch):
        e = os.spawnl(os.P_WAIT, BSDIFF, old, new, patch)

    def zipDir(self, srcDir, outputFile):
        assert os.path.isdir(srcDir)
        with zipfile.ZipFile(outputFile, "w", zipfile.ZIP_DEFLATED) as z:
            for root, dirs, files in os.walk(srcDir):
                for fn in files:
                    absfn = os.path.join(root, fn)
                    zfn = absfn[len(srcDir)+len(os.sep):]
                    z.write(absfn, zfn)
