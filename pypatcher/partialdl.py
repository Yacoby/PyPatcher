import os
import shutil
import urllib
import time
import sqlite3
import Queue as queue
from threading import Thread

class LockError(Exception):
    """
    Occures when an instance of PartialDownloader is already
    using the class
    """
    pass
    

class PartialUrlOpener(urllib.FancyURLopener):
    """
    Create sub-class in order to overide error 206.  This error means a
    partial file is being sent
    """
    def http_error_206(self, url, fp, errcode, errmsg, headers, data=None):
        pass


class PartialDownloader(Thread):
    """
    This allows us to resume downloads, so we should be able to cope
    with downloading large files or files very slowly

    It uses a sqlite database to store information on what files have been downloaded
    from what url.

    TODO: This could do with reworking, so as to remove the requirment
    to download to a single directory but instead suply a databases.
    """
    def __init__(self, configFile='downloads.sqlite'):
        Thread.__init__(self)
        self.daemon = True

        self.cfg = configFile

        #using a seperate connection for the threaded functions
        #isn't required due to the GIL
        self.con = sqlite3.connect(self.cfg)
        self.con.row_factory = sqlite3.Row
        
        self.toDownload = queue.Queue()
        self.srcDir = os.path.abspath(srcDir)

        self._sqlCreateTbl()
        self._sqlCleanDb()
        results = self._sqlGetWork()
        for r in results:
            self.toDownload.put(r)


    def _sqlCleanDb(self):
        """
        This cleans old locks that should have been unset but for 
        whatever reason haven't been
        """
        cur = self.con.cursor()
        cur.execute('''
                    UPDATE downloads
                    SET lock=0
                    WHERE last_lock < date('now', '-1 day')
                    ''')
    
    def _sqlGetWork(self):
        cur = self.con.cursor()
        cur.execute('''
                    SELECT * FROM downloads
                    WHERE lock=0
                    ''')
        return cur.fetchall()

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

    def _sqlSetActive(self, dst, active):
        """
        Sets a download as being active from the current time
        """
        cur = self.con.cursor()
        if active:
            cur.execute('''
                        UPDATE downloads
                        SET lock=1,
                            last_lock=date(\'now\')
                        WHERE dst=?
                        ''',
                        (dst,))
        else:
            cur.execute('''
                        UPDATE downloads
                        SET lock=0
                        WHERE dst=?
                        ''',
                        (dst,))

    def _sqlIsActive(self, dst):
        """
        Returns true if a download is active
        """
        cur = self.con.cursor()
        cur.execute('''
                    SELECT lock
                    FROM downloads
                    WHERE dst=?
                    ''',
                    (dst,))
        return cur.fetchone()['lock']

    def _countRow(self, row, val):
        """
        Warning: doesn't escape row
        """
        cur = self.con.cursor()
        cur.execute(( 'SELECT COUNT(*) AS count '
                    + 'FROM downloads '
                    + 'WHERE ' + row + '=?'),
                    (val,))
        return cur.fetchone()['count']

    def hasUrl(self, src):
        return self._countRow('src', src) >= 1

    def hasDst(self, dst):
        return self._countRow('dst', dst) >= 1

    def _sqlAddDl(self, src, tmp, dst):
        cur = self.con.cursor()
        cur.execute('''INSERT INTO 'downloads'
                       VALUES (?,?,?)''',
                    (src, tmp, dst))

    def _sqlRemoveDl(self, dst):
        cur = self.con.cursor()
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
                               lock int default 1,
                               last_lock datetime,
                               src varchar(255),
                               tmp varchar(255),
                               dst varchar(255) PRIMARY KEY
                           )''')

    def startDownload(self, limit=0, callback=None):
        """
        Starts downloading the queued files. 
        limit - The limit in kb/s that can be downloaded each second
        callback - The callback when the downloads are compleat. The
                    first argument to the callback is a list of downloaded
                    files
        """
        self.callback = callback
        self.limit = limit
        self.start()

    def run(self):
        downloadedFiles = []
        while not self.toDownload.empty():
            dlInfo = self.toDownload.get()

            if self._sqlIsActive(dlInfo['dst']):
                continue

            self._sqlSetActive(dlInfo['dst'], True)
            self._downloadFile(dlInfo['src'], dlInfo['tmp'])
            self._sqlSetActive(dlInfo['dst'], False)

            shutil.move(dlInfo['tmp'], dlInfo['dst'])
            self._sqlRemoveDl(dlInfo['dst'])
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
            dlSize = 10*1000
            dlStartTime = time.time()

            data = src.read(dlSize)
            if not data:
                break
            out.write(data)

            #limiter, sleeps if required
            if self.limit:
                dlTimeTaken = time.time - dlStartTime
                minDlTime = dlSize/(self.limit*1000.0)
                if timeTaken > minDlTime:
                    time.sleep(minDlTime-dlTimeTaken)

        src.close()
        out.close()

