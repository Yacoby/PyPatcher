"""
This module allows the diff and patching of directories.

It is a wrapper around bsdiff and bspatch, for lack of a better
method of doing it in python. This means that it is expcected that
bsdiff.exe and bspath.exe are on the path
"""

import urllib, os, shutil
import xmlrpclib
import glob
import threading
import difflib
import tempfile
import zipfile
import json
import hashlib
import sqlite3
import Queue as queue

#the binary patching/dif files
BSDIFF = 'bsdiff.exe'
BSPATCH = 'bspatch.exe'

#the extension for the downloaded patch files
PATCH_EXT = 'cpatch'

#the name of the database that holds the partially downloaded info 
DB_NAME = 'dl.db'

#the name of the config file that holds details about the patch
PATCH_CFG = 'cfg.json'

#dir that holds the actual patch files
PATCH_DIR = 'patchfs'

def getFileMd5(filePath):
    h = hashlib.md5.new()
    with open(filePath) as f:
        h.update(f.read())
    return h.hexdigest()

class PartialUrlOpener(urllib.FancyURLopener):
    """
    Create sub-class in order to overide error 206.  This error means a
    partial file is being sent
    """
    def http_error_206(self, url, fp, errcode, errmsg, headers, data=None):
        pass


class PartialDownlader(threading.Thread):
    """
    Basically, sometimes we want to be able to resume a download
    as some people may have slow connections or whatever.

    This allows us to resume downloads, so we should be able to cope
    with downloading large files

    It uses a sqlite database to store information on what files have been downloaded
    from what url.

    @TODO this should only allow one process to access a directory. This
    should be handled by creating a .lock file
    """
    def __init__(self, srcDir):
        c = lambda: sqlite3.connect(os.path.join(srcDir, DB_NAME))
        self.con = c()
        self.threadcon = c()
        self.toDownload = queue.Queue()

        self._sqlCreateTbl()
        results = self.con.execute('SELECT * FROM downloads').fetchall()
        for r in results:
            self.toDownload.put(r)

    def add(self, urlsrc, fileName, partialExt='.par'):
        """
        Adds a file that needs downloading. This can be called from 
        outside the thread even when the thread is running (as long
        as sqlite is fine being called from multiple processes)
        """
        #add to queue. Queue copes with threads fine
        self.toDownload.put({
            'src' : urlsrc,
            'tmp' : fileName + partialExt,
            'dst' : fileName
        }) 

        #add to db in case we need to resume
        self._sqlAddDl(urlsrc, fileName + partialExt, fileName)


    def _sqlAddDl(self, src, tmp, dst):
        cur = self.con.cursor()
        cur.execute('''INSERT INTO 'downloads'
                       (?,?,?)''', (src, tmp, dst))

    def _sqlTRemoveDl(self, dst):
        cur = self.threadcon.cursor()
        cur.execute('''DELETE FROM 'downloads'
                       WHERE dst=?''', (dst,))

    def _sqlCreateTbl(self):
        cur = self.con.cursor()
        cur.execute('''SELECT COUNT(*) AS count
                       FROM SQLITE_MASTER
                       WHERE type=\'table\'
                         AND name=\'downloads\'
                    ''')
        if cur.fetchone()['count'] == 0:
            cur = self.con.cursor()
            cur.execute('''CREATE TABLE downloads (
                               src varchar(255),
                               tmp varchar(255),
                               dst varchar(255)
                           )''')
                           
    def startDownload(self, callback=None):
        self.callback = callback
        self.start()

    def run(self):
        while not self.toDownload.empty():
            dlInfo = self.toDownload.get()
            self._downloadFile(dlInfo['src'], dlInfo['tmp'])
            shutil.move(dlInfo['tmp'], dlInfo['dst'])
            self._sqlTRemoveDl(dlInfo['dst'])

        if self.callback:
            self.callback()

    def _downloadFile(self, src, dest):
        dl = PartialUrlOpener()
        curSize = -1
        if os.path.exists(dest):
            out = open(dest,"ab")
            curSize = os.path.getsize(dest)
            dl.addheader("Range","bytes=%s-" % (curSize))
        else:
            out = open(dest,"wb")

        src = dl.open(src)
        if int(src.headers['Content-Length']) == curSize: 
            return

        while True:
            data = src.read(8192)
            if not data:
                break
            out.write(data)

        src.close()
        out.close()


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

    def patch(self, srcDir, updatesDir):
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
            self.applyPatch(srcDir, tmpDir)

            shutil.rmtree(tmpDir)

    def extract(self, inputFile, destDir):
        with zipfile.ZipFile(inputFile) as zf:
            zf.extractall(destDir)

    def applyPatch(self, srcDir, patchDir):
        """
        Given two directories, this applies the patch to srcdir from patchdir
        """
        with open(os.path.join(patchDir, PATCH_CFG)) as cfgFile:
            cfg = json.loads(cfgFile.read())

        #apply all patch files
        patchFilesDir = os.path.join(patchDir, PATCH_DIR)
        for root, dirs, files in os.walk(patchFilesDir):
            #root contains patchdir, which we don't want
            for f in files:
                
                absfn = os.path.join(root, f) 
                fn = absfn[len(patchFilesDir)+len(os.sep):]
                srcfn = os.path.join(srcDir, fn)

                filecfg = cfg[fn]

                if os.path.exists(srcfn):
                    if getFileMd5(srcfn) != filecfg['oldmd5']:
                        raise PatchError('The old patch file wasn\' correct')

                if filecfg['type'] == 'bin':
                    func = self.patchBinFile
                else:
                    func = self.patchFile
                func(srcfn, absfn)

                if getFileMd5(srcfn) != filecfg['patchedmd5']:
                    raise PatchError('There was an error patching the file')
        
        #delete files that need deleting
        for d in cfg['deleted']:
            delPath = os.path.join(srcDir, d)
            #TODO

    def patchBinFile(self, src, patch):
        e = os.spawnl(os.P_WAIT, BSPATCH, src, src, patch)
        
    def patchFile(self, src, patch):
        self.patchBinFile(src, patch)


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
