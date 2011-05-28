import os
import sys
import hashlib
import json
import shutil
from threading import Thread
import imp #for checking if frozen

import patchdiff
from partialdl import PartialDownloader

def _jsonFromFile(filePath):
    f = open(filePath)
    j = json.loads(f.read())
    f.close()
    return j

def _jsonToFile(filePath, j):
    f = open(filePath, 'w')
    f.write(json.dumps(j))
    f.close()

class BackgroundProgramPatcher:
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

    In some cases, this may need to exit the program and create a new instance
    of the program to apply patches. The communication is done using a config
    file
    """
    PATCH_JOB = 'job'
    CUR_DOWNLOADS = 'curdl'

    def __init__(self, cfgFile='patch.cfg'):
        self.cfgPath = os.path.abspath(cfgFile)

    def needsPatching(self):
        """
        Returns true if there are patches downloaded and they need
        to be applied. If this is true then it means that control
        needs to be handed to the patchProgram function to patch
        """
        if os.path.exists(self.cfgPath):
            return self.PATCH_JOB in _jsonFromFile(self.cfgPath)
        return False

    def hasPatchesDownloading(self):
        if os.path.exists(self.cfgPath):
            return self.CUR_DOWNLOADS in _jsonFromFile(self.cfgPath)
        return False

    def patchProgram(self): 
        """
        This patches the program
        """
        cfg = _jsonFromFile(self.cfgPath)

        if self.PATCH_JOB in cfg:
            srcDir   = cfg['srcdir']
            patchDir = cfg['patchdir']
            if cfg[self.PATCH_JOB] == 'applybinpatch':
                oldBin = cfg['oldbin'] 
                self._waitForExit(os.path.basename(oldBin))

                patchdiff.applyPatchDirectory(srcDir, patchDir)
                shutil.rmtree(patchDir)

                os.remove(self.cfgPath)
                os.spawnv(os.P_NOWAIT, oldBin, oldBin)
                sys.exit()
            elif cfg[self.PATCH_JOB] == 'runpatch':
                self._runPatch(srcDir, patchDir)

    def _waitForExit(self, binName):
        """
        Waits for a process to exit
        """
        pass

    def _runPatch(self, srcDir, patchDir):
        """
        Starts the patch process
        """
        if self._isFrozen():
            self._startFrozenPatch(srcDir, patchDir)
        else:
            self._runPyPatch(srcDir, patchDir)
    
    def _startFrozenPatch(self, srcDir, patchFile):
        """
        Does the first part in patching a running exe
        This copies the binary exe to a new location and runs it,
        with the argument to apply a patch
        """

        #rewrite the config file so that we change the job
        cfg = _jsonFromFile(self.cfgPath)
        cfg[self.PATCH_JOB] = 'applybinpatch'
        _jsonToFile(self.cfgPath, cfg)

        #clone the binary so that we can patch it and run it
        newName = sys.executable + '.patcher'
        if os.path.exists(newName):
            os.remove(newName)
        shutil.copy(sys.executable, newName)
        os.spawnv(os.P_NOWAIT, newName, newName)
        
        sys.exit()

    def _runPyPatch(self, srcDir, patchDir):
        """
        As python doesn't keep open python files, it is trivial
        to patch a running python file. Unpack the patch, extract
        and then restart the application
        """
        patchdiff.applyPatchDirectory(srcDir, patchDir)
        shutil.rmtree(patchDir)
        os.remove(self.cfgPath)
        os.execl(sys.executable, sys.executable, *sys.argv)

    def _isFrozen(self):
        """
        This should return true if the program that is being run is a compiled exe
        """
        return (hasattr(sys, "frozen") # new py2exe
                or hasattr(sys, "importers") # old py2exe
                or imp.is_frozen("__main__")) # tools/freeze

    def downloadAndPrePatch(self, srcDir, tmpDir,  patchDest, getPatchesFunc):
        """
        This downloads patches and does the basic work that can
        be done while the program is running (i.e. doesn't require
        any files to be replaced)

        srcDir - This is the directory to be patched
        tmpDir - This is the directory where tempary patch files are stored
        patchDest - This is the directory where the compiled patches are 
                    downloaded to
        getPatchesFunc - This funciton should get a list of patch urls, in the
                         order they should be applied
        """
        cfg = _jsonFromFile(self.cfgPath)
        if self.CUR_DOWNLOADS in cfg:
            patchInfo = cfg[self.CUR_DOWNLOADS]
        else:
            patchInfo = getPatchesFunc()
            cfg[self.CUR_DOWNLOADS] = patchInfo
            _jsonToFile(self.cfgPath, cfg)

        urlToName = lambda x: hashlib.md5(x).hexdigest() 

        def prePatch():
            """
            This is run in another another thread (the download thread) 
            This function runs the patches in a directory and setups
            the job for the next startup
            """
            patchFiles = []
            for p in patchInfo:
                patchFiles.append(os.path.join(patchDest, urlToName(p)))
            patchdiff.mergePatches(srcDir, tmpDir, patchFiles)

            #trash and rewrite the config
            cfg = {}
            cfg[self.PATCH_JOB] = 'runpatch'
            cfg['srcdir'] = srcDir
            cfg['patchdir'] = tmpDir 
            _jsonToFile(self.cfgPath, cfg)

        dl = PartialDownloader(patchDest)
        for update in patchInfo:
            dl.add(update, urlToName(update))
        dl.startDownload(prePatch)
