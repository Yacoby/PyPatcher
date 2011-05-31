import os
import sys
import hashlib
import json
import shutil
from threading import Thread
import imp #for checking if frozen
import traceback

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

class Error(Exception):
    pass

class BrokenError(Exception):
    """
    This is in the case that the patching process is borked.
    """
    pass

class ProgramPatcher:
    """
    This contains the core functions for patching a program and is designed
    to be run from the program being patched

    When running as a python script, this isn't a problem. When running as an
    exe it is as we can't replace a running exe. In this case, the solution is
    to copy the exe, and run the patch from then, then cleaning it up afterwards
    by running the main program. This class is designed to help with that task. 

    It is expected that much of the patch work will be done while the user is
    running the program, and at startup all that will need to happen is that
    the program will need to move some files. Hence the patching is split into
    two functions.

    In some cases, this may need to exit the program and create a new instance
    of the program to apply patches. The communication is done using a config
    file
    """
    PATCH_JOB = 'job'
    BROKE = 'borked'

    def __init__(self, cfgFile='patch.cfg'):
        self.cfgPath = os.path.abspath(cfgFile)
        if not os.path.exists(os.path.dirname(self.cfgPath)):
            raise Error('The config path doesn\'t exist')

    def needsPatching(self):
        """
        Returns true if there are patched files that need to be
        applied. If this is true then it means that control needs to be handed
        to the patchProgram function to patch
        """
        if os.path.exists(self.cfgPath):
            cfg = _jsonFromFile(self.cfgPath)
            return self.PATCH_JOB in cfg and not self.BROKE in cfg
        return False

    def _setBroken(self):
        try:
            cfg = _jsonFromFile(self.cfgPath)
            cfg[self.BROKE] = True
            _jsonToFile(self.cfgPath, cfg)
        except:
            pass

    def isBroken(self):
        if os.path.exists(self.cfgPath):
            return self.BROKE in _jsonFromFile(self.cfgPath)
        return False

    def patchProgram(self): 
        """
        This patches the program

        This catches every error that might occur and rethrows. This is because
        we don't want patching to get in the way of program starting
        """
        try:
            if self.isBroken():
                raise Error('Cannot patch if broken')

            cfg = _jsonFromFile(self.cfgPath)

            if self.PATCH_JOB in cfg:
                if cfg[self.PATCH_JOB] == 'applybinpatch':
                    self._finishFrozenPatch()
                elif cfg[self.PATCH_JOB] == 'runpatch':
                    self._runPatch(cfg['srcdir'], cfg['patchdir'])

        except (BrokenError, Error):
            traceback.print_exc(file='patcherr.log')
            self._setBroken()
            raise
        #unknown error
        except:
            traceback.print_exc(file='patcherr.log')
            self._setBroken()
            raise BrokenError('There was an unknown error')

    def prePatchProgram(self, srcDir, tmpDir, patches):
        """
        This does work that can be done while the program is running
        such as generating the patched files
        """
        try:
            patchdiff.mergePatches(srcDir, tmpDir, patches)
        except patchdiff.PatchError:
            raise Error(( 'There was an error encounted generating '
                        + 'patch files'))

        #trash and rewrite the config.
        try:
            cfg = {}
            cfg[self.PATCH_JOB] = 'runpatch'
            cfg['srcdir'] = srcDir
            cfg['patchdir'] = tmpDir 
            _jsonToFile(self.cfgPath, cfg)
        except:
            raise Error(( 'There was an error storing the config file. The '
                        + 'generated patches won\'t be applied' ))

    def _runPatch(self, srcDir, patchDir):
        """
        Starts the patch process, depending on which method is required
        """
        if self._isFrozen():
            self._startFrozenPatch(srcDir, patchDir)
        else:
            self._runPyPatch(srcDir, patchDir)
    
    def _startFrozenPatch(self, srcDir, patchFile):
        """
        Does the first part in patching a running exe This copies the binary
        exe to a new location and runs it, with the argument to apply a patch
        """
        #rewrite the config file so that we change the job
        cfg = _jsonFromFile(self.cfgPath)
        cfg[self.PATCH_JOB] = 'applybinpatch'
        cfg['oldbin'] = sys.executable
        _jsonToFile(self.cfgPath, cfg)

        #clone the binary so that we can patch it and run it
        newName = sys.executable + '.patcher'
        if os.path.exists(newName):
            os.remove(newName)
        shutil.copy(sys.executable, newName)
        os.execlp(newName, newName)
        
        sys.exit()
    
    def _finishFrozenPatch(self):
        cfg = _jsonFromFile(self.cfgPath)
        oldBin = cfg['oldbin']
        srcDir = cfg['srcdir']
        patchDir = cfg['patchdir']

        try:
            patchdiff.applyPatchDirectory(srcDir, patchDir)
        except patchdiff.PatchError:
            self._setBroken()
            raise BrokenError(('There was an error patching.'
                             + ' See patcherr.log for more information'))

        self._removeCfgFile()

        shutil.rmtree(patchDir)
        os.execlp(oldBin, oldBin)
        sys.exit()

    def _removeCfgFile(self):
        """
        This must be removed or at least set to nothing
        as if it isn't we may get stuck in a loop of trying to 
        patch the program
        """
        try:
            os.remove(self.cfgPath)
        except:
            try:
                _jsonToFile(self.cfgPath, [])
            except:
                raise BrokenError(('The config file couldn\'t be deleted or'
                                 + ' written to'))

    def _runPyPatch(self, srcDir, patchDir):
        """
        As python doesn't keep open python files, it is trivial to patch a
        running python file. Unpack the patch, extract and then restart the
        application
        """
        try:
            patchdiff.applyPatchDirectory(srcDir, patchDir)
        except patchdiff.PatchError:
            self._setBroken()
            raise BrokenError(('There was an error patching.'
                             + ' See patcherr.log for more information'))

        self._removeCfgFile()

        shutil.rmtree(patchDir)
        os.spawnlp(sys.executable, sys.executable, *sys.argv)
        sys.exit()

    def _isFrozen(self):
        """
        This should return true if the program that is being run is a compiled
        exe
        """
        return (hasattr(sys, "frozen") # new py2exe
                or hasattr(sys, "importers") # old py2exe
                or imp.is_frozen("__main__")) # tools/freeze


class BackgroundProgramPatcher(ProgramPatcher):
    """
    This class is designed to mimic chromes patching behavior in that
    it downloads the files in the background
    """
    CUR_DOWNLOADS = 'curdl'

    def hasPatchesDownloading(self):
        if os.path.exists(self.cfgPath):
            cfg = _jsonFromFile(self.cfgPath)
            return (self.CUR_DOWNLOADS in _jsonFromFile(self.cfgPath)
                    and not self.BROKE in cfg)
        return False

    def downloadAndPrePatch(self, srcDir, tmpDir,  patchDest,
                                  getPatchesFunc, dlLim=0):
        """
        This downloads patches and does the basic work that can be done while
        the program is running (i.e. doesn't require any files to be replaced)

        srcDir - This is the directory to be patched
        tmpDir - This is the directory where tempary patch files are stored
        patchDest - This is the directory where the compiled patches are 
                    downloaded to
        getPatchesFunc - This funciton should get a list of patch urls, in the
                         order they should be applied. This function should
                         call the function passed to it as its first argument
        """
        if self.isBroken():
            raise Error('Cannot download patchs if broken')

        if not os.path.exists(srcDir):
            raise Error('The src dir doesn\'t exist')
        if not os.path.isdir(srcDir):
            raise Error('The src dir isn\'t a directory')

        if not os.path.exists(tmpDir):
            os.makedirs(tmpDir)

        if not os.path.exists(patchDest):
            os.makedirs(patchDest)


        #hack for partial function application
        cb = lambda files: self._downloadPrePatch(srcDir,
                                                  tmpDir,
                                                  patchDest,
                                                  files,
                                                  dlLim)
        if os.path.exists(self.cfgPath):
            cfg = _jsonFromFile(self.cfgPath)
            if self.CUR_DOWNLOADS in cfg:
                cb(cfg[self.CUR_DOWNLOADS])
            else:
                getPatchesFunc(cb)
        else:
            getPatchesFunc(cb)

    def _downloadPrePatch(self, srcDir, tmpDir, patchDest, files, limit):
        if not files:
            return

        cfg = _jsonFromFile(self.cfgPath)
        cfg[self.CUR_DOWNLOADS] = files
        _jsonToFile(self.cfgPath, cfg)

        urlToName = lambda x: hashlib.md5(x).hexdigest() 
        def prePatch(dlFiles):
            """
            This is run in another another thread (the download thread) 
            This function runs the patches in a directory and setups the job
            for the next startup

            The one thing to be awear of is that the files downloaded may not
            be all the files as if another program is downloading some of them
            they won't be downloaded by this process. Hence it is required
            to check if the files exist 
            """
            patchFiles = []
            for p in files:
                f = os.path.join(patchDest, urlToName(p))
                if not os.path.exists(f):
                    return
                patchFiles.append(f)

            self.prePatchProgram(srcDir, tmpDir, patchFiles)

        #setup downloader and download all required files
        dl = PartialDownloader()
        for update in files:
            dl.add(update, os.path.join(patchDest, urlToName(update)))
        dl.startDownload(limit, prePatch)
