import os

from patcher import BackgroundProgramPatcher, BrokenError, Error

def getPatches(callback):
    #download list of update urls 
    updates = []

    #give the patcher the updates
    #as it is callback based, we
    #could have downloaded patches
    #in another thread
    callback(updates)

try:
    pb = BackgroundProgramPatcher()
    if pb.needsPatching():
        pb.patchProgram()
    else:
        pb.downloadAndPrePatch(os.getcwd(),
                               os.path.abspath('patcher'),
                               os.path.abspath('dlpatches'),
                               getPatches,
                               10) #limit to 10kb/s
except BrokenError:
    #This error occurres when there has been an error that has broken
    #the install process
    pass
except Error:
    #this error occures when something has gone wrong, but maybe
    #not anything that invalidates the install process
    pass
