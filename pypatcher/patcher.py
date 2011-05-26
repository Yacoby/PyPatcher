import os
import json
import shutil
from threading import Thread

import patchdiff

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
    """
    def __init__(self, cfgFile='patch.cfg'):
        self.cfgPath = os.path.abspath(cfgFile)

    def needsPatching(self):
        if os.path.exists(self.cfgPath):
            with open(self.cfgPath) as fh:
                cfg = json.loads(cfgFile.readlines())
                return 'job' in cfg and cfg['job'] == 'runpatch'
        return False

    def patchProgram(self): 
        fh = open(self.cfgPath)
        cfg = json.loads(fh.readlines())
        fh.close()

        if 'job' in cfg:
            srcDir   = cfg['srcdir']
            patchDir = cfg['patchdir']
            if cfg['job'] == 'applybinpatch':
                oldBin   = cfg['oldbin'] 
                self.waitForExit(os.path.basename(oldBin))

                patchdiff.applyPatchDirectory(srcDir, patchDir)
                shutil.rmtree(patchDir)

                os.remove(cfgFile)
                os.popenv(os.P_NOWAIT, oldBin, oldBin)
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
        patchdiff.applyPatchDirectory(srcDir, patchDir)
        shutil.rmtree(patchDir)
        os.remove(cfgName)
        os.execl(sys.executable, sys.executable, *sys.argv)

    def exit(self):
        sys.exit()

    def isFrozen(self):
        """
        This should return true if the program that is being run is a compiled exe
        """
        return (hasattr(sys, "frozen") # new py2exe
                or hasattr(sys, "importers") # old py2exe
                or imp.is_frozen("__main__")) # tools/freeze

    def downloadAndPrePatch(self, urlSrc):
        """
        This downloads patches and does the basic work that can
        be done while the program is running (i.e. doesn't require
        any files to be replaced)
        """
        d = Downloader()
        d.downloadUpdates(urlSrc, destDir, curVer, self.prePath)
        
    def prePatch(self, downloadedFiles):
        """
        This is run in another another thread (the download thread) as it
        is the callback from the above function. This function runs the
        patches in a directory and setups the job for the next startup
        """
        srcDir = ?
        outDir = tempfile.mkdtemp()
        patchdiff.mergePatches(srcDir, outDir, patchFiles)

        cfg = {}
        cfg['job'] = 'runpatch'
        cfg['srcdir'] = srcDir
        cfg['patchdir'] = outDir
        fh = open(self.cfgPath, 'w')
        fh.write(json.dumps(cfg))
        fh.close()
        

class Downloader(Thread):
    """
    This is a threaded class that allows patches to be downloaded in
    the background. 

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
