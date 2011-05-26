"""
This module allows the diff and patching of directories.

It is a wrapper around bsdiff and bspatch, for lack of a better
method of doing it in python. This means that it is expcected that
bsdiff.exe and bspath.exe are on the path

the patch file is compressed into a simple .zip which holds
a config file (listing deleted files and how each patch file
was generated) a load of patch files (in PATCH_DIR) and a load
of new files in NEW_DIR

The design of this is intedned so you can apply multiple patches
to a set of files in a directory and save to an output directory.
Hence patching files is a two stage progess, first you generate a
set of patched files/merged configs in a temp directory and then
you merge the temp directory and the origanal source directory
"""

import os
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
NEW_DIR = 'newfs'
MERGED_FILES = 'files'

def getFileMd5(filePath):
    h = hashlib.md5.new()
    with open(filePath) as f:
        h.update(f.read())
    return h.hexdigest()

#------------------------------------------------------------------------------
#Patch functions

class PatchError(Exception):
    """
    A generic error occuring upon patching a file
    """
    pass


def applyPatchDirectory(srcDir, patchDir):
    """
    Given a soruce directory and a directory generated using
    mergePatches, this merges srcDir and patchDir, deleing
    the files listed in the cfg file
    """
    filesDir = os.path.join(patchDir, MERGED_FILES)
    for root, dirs, files in os.walk(filesDir):
        for f in files:
            absFn = os.path.join(root, f) 
            fn = absFn[len(filesDir)+len(os.sep):]
            toAbsFn = os.path.join(srcDir, fn)

            if os.path.exists(toAbsFn):
                os.remove(toAbsFn)
            shutil.move(absFn, toAbsFn)

    #run deletions
    fh = open(os.path.join(patchDir,PATCH_CFG) 
    cfg = json.loads(fh.read())
    fh.close()
    for f in cfg['deleted']:
        os.remove(os.path.join(srcDir, f))

def mergePatches(srcDir, outDir, patchFiles):
    """
    This when given a set of pages and a source directory applies the
    patches and puts the output in a output directory.
    """
    deletedFiles = []
    for f in patchFiles:
        tmpDir = tempfile.mkdtemp() 

        _extract(f, tmpDir)
        _applyPatch(srcDir, outDir, tmpDir, delList)

        shutil.rmtree(tmpDir)

    fh = open(os.path.join(outDir,PATCH_CFG), 'w') 
    fh.write(json.dumps({'deleted' : deleted}))
    fh.close()

def _extract(inputFile, destDir):
    with zipfile.ZipFile(inputFile) as zf:
        zf.extractall(destDir)

def _applyPatch(srcDir, outDir, patchDir, delList):
    """
    Given two directories, this applies the patch to srcdir from patchdir
    If the file to be patched already exists in ouputDir, then the patch
    will be run on the file in outputDir.

    delList is a list of files that have been deleted. If a file
    that had been deleted is again created then it is removed
    from the deleted list
    """
    with open(os.path.join(patchDir, PATCH_CFG)) as cfgFile:
        cfg = json.loads(cfgFile.read())

    #move all new files
    newFilesDir = os.path.join(patchDir, NEW_DIR)
    for root, dirs, files in os.walk(newFilesDir):
        for f in files:
            absFn = os.path.join(root, f) 
            fn = absFn[len(newFilesDir)+len(os.sep):]
            outAbsFn = os.path.join(outDir, MERGED_FILES, fn)
            if os.path.exists(outAbsFn):
                os.remove(outAbsFn)
            shutil.copy(absFn, outAbsFn)

            if fn in delList:
                delList.remove(fn)

    #apply all patch files
    patchFilesDir = os.path.join(patchDir, PATCH_DIR)
    for root, dirs, files in os.walk(patchFilesDir):
        #root contains patchdir, which we don't want
        for f in files:
            absFn = os.path.join(root, f) 
            fn = absFn[len(patchFilesDir)+len(os.sep):]
            srcAbsFn = os.path.join(srcDir, fn)
            outAbsFn = os.path.join(outDir, MERGED_FILES, fn)

            filecfg = cfg[fn]

            if os.path.exists(outAbsFn):
                toPatchAbsFn = outAbsFn
            else:
                toPatchAbsFn = srcAbsFn
                
            if os.path.exists(toPatchAbsFn):
                if getFileMd5(toPatchAbsFn) != filecfg['oldmd5']:
                    raise PatchError('The old patch file wasn\' correct')

            if filecfg['type'] == 'bin':
                func = _patchBinFile
            else:
                func = _patchFile
            func(toPatchAbsFn, absFn, outAbsFn)

            if getFileMd5(outAbsFn) != filecfg['patchedmd5']:
                raise PatchError('There was an error patching the file')
    
    #add deleted
    delFiles.extend(cfg['deleted'])

def _patchBinFile(src, out, patch):
    e = os.spawnl(os.P_WAIT, BSPATCH, src, out, patch)
    
def _patchFile(src, out, patch):
    _patchBinFile(src, out, patch)

#------------------------------------------------------------------------------
#Diff functions

class DiffError(Exception):
    pass


def generateDiff(oldDir, newDir, outputFile):
    """
    Generates a patch containing the diff between two directories
    """
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
            fn = _genFile
            if fn.endswith('.exe'):
                filecfg['types'] = 'bin'
                fn = _genBinFile

            fn(os.path.join(oldDir, fn),
               os.path.join(newDir, fn),
               os.path.join(tmpDir, PATCH_DIR , fn))
    
    cfgOut = os.path.join(tmpDir, PATCH_CFG)
    open(cfgOut).write(json.dumps(cfg)).close()
    _zipDir(tmpDir, outputFile)

    shutil.rmtree(tmpDir)

def _genFile(old, new, patch):
    o = open(old)
    n = open(new)
    p = open(patch, 'w')
    p.writelines(difflib.ndiff(o.readlines(),
                               n.readlines()))
    o.close()
    n.close()
    p.close()

def _genBinFile(old, new, patch):
    e = os.spawnl(os.P_WAIT, BSDIFF, old, new, patch)

def _zipDir(srcDir, outputFile):
    assert os.path.isdir(srcDir)
    with zipfile.ZipFile(outputFile, "w", zipfile.ZIP_DEFLATED) as z:
        for root, dirs, files in os.walk(srcDir):
            for fn in files:
                absfn = os.path.join(root, fn)
                zfn = absfn[len(srcDir)+len(os.sep):]
                z.write(absfn, zfn)
