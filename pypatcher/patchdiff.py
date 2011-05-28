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
import shutil
import tempfile
import zipfile
import json
import hashlib
import string

from diffmatchpatch import diff_match_patch 

from partialdl import PartialDownloader

#the binary patching/dif files
BSDIFF = 'bsdiff'
BSPATCH = 'bspatch'

#the name of the config file that holds details about the patch
PATCH_CFG = 'cfg.json'

#dir that holds the actual patch files
PATCH_DIR = 'patchfs'
NEW_DIR = 'newfs'
MERGED_FILES = 'files'

def _getFileContents(filePath, mode='r'):
    f = open(filePath, mode)
    contents = f.read()
    f.close()
    return contents

def _getFileMd5(filePath):
    h = hashlib.md5()
    h.update(_getFileContents(filePath))
    return h.hexdigest()

def _mkdirs(d):
    """
    Makes directories, but doesn't throw an error if
    the directory exists
    """
    if not os.path.exists(d):
        os.makedirs(d)

def _createCopy2(src, dst):
    """
    Creates a of a file but ensures directories
    exist and overwrites the dst if it exists
    """
    _mkdirs(os.path.dirname(dst))
    if os.path.exists(dst):
        os.remove(dst)
    shutil.copy2(src, dst)

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
    fh = open(os.path.join(patchDir,PATCH_CFG))
    cfg = json.loads(fh.read())
    fh.close()
    for f in cfg['deleted']:
        os.remove(os.path.join(srcDir, f))

def mergePatches(srcDir, outDir, patchFiles):
    """
    This when given a set of pages and a source directory applies the
    patches and puts the output in a output directory.
    """
    _mkdirs(outDir)

    delList = []
    for f in patchFiles:
        tmpDir = tempfile.mkdtemp() 

        _extract(f, tmpDir)
        _applyPatch(srcDir, outDir, tmpDir, delList)

        shutil.rmtree(tmpDir)

    fh = open(os.path.join(outDir,PATCH_CFG), 'w') 
    fh.write(json.dumps({'deleted' : delList}))
    fh.close()

def _extract(inputFile, destDir):
    assert ( os.path.isfile(inputFile) )
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
            _createCopy2(absFn, outAbsFn)

            if fn in delList:
                delList.remove(fn)

    #apply all patch files
    patchFilesDir = os.path.join(patchDir, PATCH_DIR)
    for root, dirs, files in os.walk(patchFilesDir):
        #root contains patchdir, which we don't want
        for f in files:
            patchAbsFn = os.path.join(root, f) 
            fn = patchAbsFn[len(patchFilesDir)+len(os.sep):]
            srcAbsFn = os.path.join(srcDir, fn)
            outAbsFn = os.path.join(outDir, MERGED_FILES, fn)

            filecfg = cfg[fn]

            if os.path.exists(outAbsFn):
                toPatchAbsFn = outAbsFn
            else:
                toPatchAbsFn = srcAbsFn
                
            if os.path.exists(toPatchAbsFn):
                if _getFileMd5(toPatchAbsFn) != filecfg['oldmd5']:
                    raise PatchError(('The file ' + toPatchAbsFn + ' has changed'
                                    + ' so cannot be patched'))
            else:
                raise PatchError(('The file ' + toPatchAbsFn + ' doesn\' exist'
                                + ' so cannot be patched'))
                

            if filecfg['type'] == 'bsdiff':
                func = _patchBin
            elif filecfg['type'] == 'text':
                func = _patchText
            else:
                raise PatchError('Unknown type')
            func(toPatchAbsFn, outAbsFn, patchAbsFn)

            if not os.path.exists(outAbsFn):
                raise PatchError('The output from patching: ' + outAbsFn + ' doesn\'t exist')

            if _getFileMd5(outAbsFn) != filecfg['patchedmd5']:
                raise PatchError('There was an error patching the file: ' + toPatchAbsFn)
    
    #add deleted
    delList.extend(cfg['deleted'])

def _patchBin(src, out, patch):
    assert ( os.path.exists(src) )
    assert ( os.path.exists(patch) )

    _mkdirs(os.path.dirname(out))
    e = os.spawnlp(os.P_WAIT, BSPATCH, BSPATCH, src, out, patch)
    if e != 0:
        raise PatchError('Error when using ' + BSPATCH + ' to patch ' + src)
    
def _patchText(src, out, patch):
    o = diff_match_patch()
    txt = _getFileContents(src)
    patchTxt = _getFileContents(patch)

    #the result object is a tuple, the first element
    #is the patched text and the second is an array
    #of boolean values indicating which patches
    #were applied
    result = o.patch_apply(o.patch_fromText(patchTxt), txt)

    if not all(result[1]):
        raise PatchError(('Not all patches were applied when patching'
                        + 'the file ' + src + ' with patch ' + patch))

    outTxt = result[0]
    
    _mkdirs(os.path.dirname(out))
    f = open(out, 'w')
    f.write(outTxt)
    f.close()

#------------------------------------------------------------------------------
#Diff functions

class DiffError(Exception):
    pass


def _isText(filePath):
    """
    This roughly follows perls idea of checking file types.
    A text file can't have nulls, and the amount of non text
    ascii has to be less than 30%
    """
    textCharacters = "".join(map(chr, range(32, 127)) + list("\n\r\t\b"))
    nullTrans = string.maketrans("", "")
    with open(filePath) as fh:
        out = fh.read(1024)
        if not out:
            return True

        if "\0" in out:
            return False
        tran = out.translate(nullTrans, textCharacters)
        return float(len(tran))/len(out) < 0.3
    return False

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

            filecfg['patchedmd5'] = _getFileMd5(absfn)
            if not os.path.exists(oldfn):
                _createCopy2(absfn,
                             os.path.join(tmpDir, NEW_DIR , fn))
            else:
                filecfg['oldmd5'] = _getFileMd5(oldfn)

                if _isText(absfn):
                    filecfg['type'] = 'text'
                    func = _genTextPatch
                else: #use bsdiff for anything with think is binary
                    filecfg['type'] = 'bsdiff'
                    func = _genBinPatch

                func(os.path.join(oldDir, fn),
                     os.path.join(newDir, fn),
                     os.path.join(tmpDir, PATCH_DIR , fn))
    
    cfgOut = os.path.join(tmpDir, PATCH_CFG)
    _mkdirs(os.path.dirname(cfgOut))
    with open(cfgOut, 'w') as f:
        f.write(json.dumps(cfg))
    _zipDir(tmpDir, outputFile)

    assert ( os.path.exists(outputFile) )
    assert ( os.path.isfile(outputFile) )

    shutil.rmtree(tmpDir)

def _genTextPatch(old, new, patch):
    oldTxt = _getFileContents(old)
    newTxt = _getFileContents(new)
    
    o = diff_match_patch()
    patchTxt = o.patch_toText(o.patch_make(oldTxt, newTxt))

    _mkdirs(os.path.dirname(patch))
    f = open(patch, 'w')
    f.write(patchTxt)
    f.close()

def _genBinPatch(old, new, patch):
    assert ( os.path.exists(old) and os.path.isfile(old) )
    assert ( os.path.exists(new) and os.path.isfile(new) )

    assert ( not os.path.exists(patch) )

    _mkdirs(os.path.dirname(patch))
    assert ( os.path.exists(os.path.dirname(patch)) )

    e = os.spawnlp(os.P_WAIT, BSDIFF, BSDIFF, old, new, patch)
    if e != 0:
        raise DiffError((BSDIFF + ' did not run sucessfully when generating'
                       + ' patches: ' + old + ' ' + new + ' ' + patch))

def _zipDir(srcDir, outputFile):
    assert os.path.isdir(srcDir)
    with zipfile.ZipFile(outputFile, "w", zipfile.ZIP_DEFLATED) as z:
        for root, dirs, files in os.walk(srcDir):
            for fn in files:
                absfn = os.path.join(root, fn)
                zfn = absfn[len(srcDir)+len(os.sep):]
                z.write(absfn, zfn)
