import os
import shutil
import urllib
import sqlite3
import Queue as queue
from threading import Thread

#the name of the database that holds the partially downloaded info 
DB_NAME = 'dl.db'

class PartialUrlOpener(urllib.FancyURLopener):
    """
    Create sub-class in order to overide error 206.  This error means a
    partial file is being sent
    """
    def http_error_206(self, url, fp, errcode, errmsg, headers, data=None):
        pass


class PartialDownlader(Thread):
    """
    Basically, sometimes we want to be able to resume a download
    as some people may have slow connections or whatever.

    This allows us to resume downloads, so we should be able to cope
    with downloading large files

    It uses a sqlite database to store information on what files have been downloaded
    from what url.

    """
    def __init__(self, srcDir):
        c = lambda: sqlite3.connect(os.path.join(srcDir, DB_NAME))
        self.con = c()
        self.threadcon = c()
        
        self.toDownload = queue.Queue()
        self.srcDir = os.path.abspath(srcDir)

        lockFn = os.path.join(srcDir, '.lock')
        if os.path.exists(lockFn):
            raise Exception('Already an instance downloading')

        open(lockFn, 'w').close()

        self._sqlCreateTbl()
        results = self.con.execute('SELECT * FROM downloads').fetchall()
        for r in results:
            self.toDownload.put(r)

    def __del__(self):
        os.remove(os.path.join(self.srcDir, '.lock'))

    def add(self, urlsrc, fileName, partialExt='.par'):
        """
        Adds a file that needs downloading. This can be called from 
        outside the thread even when the thread is running (as long
        as sqlite is fine being called from multiple processes)
        """
        path = os.path.join(self.srcDir, fileName)
        #add to queue. Queue copes with threads fine
        self.toDownload.put({
            'src' : urlsrc,
            'tmp' : path + partialExt,
            'dst' : path 
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
        downloadedFiles = []
        while not self.toDownload.empty():
            dlInfo = self.toDownload.get()
            self._downloadFile(dlInfo['src'], dlInfo['tmp'])
            shutil.move(dlInfo['tmp'], dlInfo['dst'])
            self._sqlTRemoveDl(dlInfo['dst'])
            downloadedFiles.append(dlInfo['dst'])

        if self.callback:
            self.callback(downloadedFiles)

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

