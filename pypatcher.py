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

class ProgramPatcher:
    """
    This class attempts to patch a program. This can be run from the program
    being patched.

    When running as a python script, this isn't a problem. When running as an exe
    it is as we can't replace a running exe. In this case, the solution is to copy
    the exe, and run the patch from then, then cleaning it up afterwards by running
    the main program. This class is designed to help with that task. 

    It is expected that much of the patch work will be done while
    the user is running the program, and at startup all that will
    need to happen is that the program will need to move some files.
    """
    def __init__(self, cfgFile='patch.cfg'):
        self.cfgPath = os.path.abspath(cfgFile)

    def needsPatching(self):
        if os.path.exists(self.cfgPath):
            with open(self.cfgPath) as fh:
                cfg = json.loads(cfgFile.readlines())
                return 'job' in cfg and cfg['job'] == 'runpatch'
        return False

    def patchProgram(self,cfgFile='patch.cfg'): 
        fh = open(self.cfgPath)
        cfg = json.loads(fh.readlines())
        fh.close()

        if 'job' in cfg:
            srcDir   = cfg['srcdir']
            patchDir = cfg['patchdir']
            if cfg['job'] == 'applybinpatch':
                oldBin   = cfg['oldbin'] 
                self.waitForExit(os.path.basename(oldBin))
                self.applyPatchedFiles(srcDir, patchDir)
                self.cleanTmp(patchDir)
                os.popenv(os.P_NOWAIT, oldBin, oldBin)
                os.remove(cfgFile)
                self.exit()
            elif cfg['job'] == 'runpatch':
                self.runPatch(srcDir, patchDir)


    def waitForExit(self, binName):
        pass

    def runPatch(self, cfgName, srcDir, patchDir):
        """
        Starts the patch process
        """
        if self.isFrozen():
            self.runFrozenPatch(srcDir, patchDir)
        else:
            self.runPyPatch(cfgName, srcDir, patchDir)
    
    def runFrozenPatch(self, srcDir, patchFile):
        """
        Does the first part in patching a running exe
        This copies the binary exe to a new location and runs it,
        with the argument to apply a patch
        """

        #rewrite the config file so that we change the job
        fh = open(self.cfgPath)
        cfg = json.loads(fh.read())
        cfg['job'] = 'applybinpatch'
        fh.close()

        fh = open(self.cfgPath, 'w')
        fh.write(json.dumps(cfg))
        fh.close()

        #clone the binary so that we can patch it and run it
        newName = sys.executable + '.patcher'
        if os.path.exists(newName):
            os.remove(newName)
        shutil.copy(sys.executable, newName)
        os.popenv(os.P_NOWAIT, newName, newName, '--applypatch')
        
        self.exit()

    def runPyPatch(self, cfgName, srcDir, patchDir):
        """
        As python doesn't keep open python files, it is trivial
        to patch a running python file. Unpack the patch, extract
        and then restart the application
        """
        self.applyPatchedFiles(srcDir, patchDir)
        self.cleanTmp(patchDir)
        os.remove(cfgName)
        os.execl(sys.executable, sys.executable, *sys.argv)

    def exit(self):
        sys.exit()

    def applyPatchedFiles(self, srcDir, tmpDir):
        """
        This copys patch files to the new directory, overwriting files that already exist
        then it deletes the files in the patch directory
        """
        for root, dirs, files in os.walk(tmpDir):
            for f in files:
                absFn = os.path.join(root, f) 
                fn = absFn[len(tmpDir)+len(os.sep):]

                toAbsFn = os.path.join(srcDir, fn)

                if os.path.exists(toAbsFn):
                    os.remove(toAbsFn)
                shutil.move(absFn, toAbsFn)

        shutil.rmtree(tmpDir)


    def isFrozen(self):
        """
        This should return true if the program that is being run is a compiled exe
        """
        return (hasattr(sys, "frozen") # new py2exe
                or hasattr(sys, "importers") # old py2exe
                or imp.is_frozen("__main__")) # tools/freeze

    def downloadAndPrePatch(self, urlSrc):
        """
        This downloads patches and does the basic work applying the first patch
        """
        d = Downloader()
        d.downloadUpdates(urlSrc, destDir, curVer, self.prePath)
        
    def prePatch(self):
        """
        This is run in another another thread (the download thread) as it
        is the callback from the above function. This function runs the
        patches in a directory and setups the job for the next startup
        """
        p = Patcher()
        p.patch(srcDir, updatesDir, outputDir)

        cfg = {}
        cfg['job'] = 'runpatch'
        cfg['srcdir'] = 
        cfg['patchdir'] = outputDir
        fh = open(self.cfgPath, 'w')
        fh.write(json.dumps(cfg))
        fh.close()
        

class Downloader(threading.Thread):
    """
    This is a threaded class that allows patches to be downloaded in
    the background. This allows seamless updating.

    It is expected that the urlSrc is a xmlrpc server
    """
    def downloadUpdates(self, urlSrc, destDir, curVer, callback=None):
        """
        This downloads updates into the directory dest. If there is no newer
        version then nothing happens
        """
        self.urlSrc = urlSrc
        self.destDir = urlSrc
        self.curVer = urlSrc
        self.callback = callback 
        self.start()

    def run(self):
        dl = PartialDownlader(self.destDir)
        rpc = xmlrpclib.ServerProxy(self.urlSrc)
        updates = rpc.getUpdateUrls(self.curVer)
        for update in updates:
            dl.add(update['url'], update['version'] + '.' + PATCH_EXT)
        dl.startDownload(self.callback)


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
