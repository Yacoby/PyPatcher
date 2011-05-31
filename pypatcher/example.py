import os
import patcher

#------------------------------------------------------------------------------
#A Google Chrome style patching program. This would go as early in the program as
#possible


def getPatches(callback):
    #download list of update urls 
    updates = []

    #give the patcher the updates
    #as it is callback based, we
    #could have downloaded patches
    #in another thread
    callback(updates)

try:
    pb = patcher.BackgroundProgramPatcher()

    #checks if there are patched files that need to be moved over
    if pb.needsPatching():
        pb.patchProgram()
    else:
        #                      #directory to be patched
        pb.downloadAndPrePatch(os.getcwd(), 

                               #directory where patched files go
                               #(this is tempoary, and only needs to exist
                               #between a program shutdown and startup)
                               os.path.abspath('patcher'), 

                               #directory where the patches are downloaded to
                               os.path.abspath('dlpatches'), 

                               #a function that returns a list of patch urls,
                               #this isn't called if patches are already
                               #being downloaded
                               getPatches,

                               #limit the download speed to 10kb/s
                               10) 
except patcher.BrokenError:
    #This error occurres when there has been an error that has broken
    #the install process
    pass
except patcher.Error:
    #this error occures when something has gone wrong, but maybe
    #not anything that invalidates the install process
    pass

#------------------------------------------------------------------------------
#Simple example without the more complex downloader
try:
    pb = patcher.ProgramPatcher()
    pb.prePatchProgram(os.getcwd(),
                       os.path.abspath('patcher'),
                       ['list', 'of', 'patch', 'files'])
    pb.patchProgram()
except patcher.BrokenError:
    pass
except patcher.Error:
    pass
