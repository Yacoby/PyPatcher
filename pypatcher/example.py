from patcher import BackgroundProgramPatcher

def getPatches(callback):
    #download list of update urls 
    updates = []

    #give the patcher the updates
    callback(updates)

pb = BackgroundProgramPatcher()
if pb.needsPatching():
    pb.patchProgram()
else:
    pb.downloadAndPrePatch(os.getcwd(),
                           os.path.abspath('patcher'),
                           os.path.abspath('dlpatches'),
                           getPatches)
