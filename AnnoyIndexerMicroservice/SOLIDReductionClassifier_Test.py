'''
Test Data
'''


import time
import os
import zipfile
import requests

# URL for getting the annoy indexer from
SERVER_URI = 'http://127.0.0.1:5000'
UPLOAD_SERVER_URI = 'http://upload-files-ms.eba-rvniqqiy.eu-central-1.elasticbeanstalk.com/'
FM_SERVER_URI = 'http://slim-feature-map-ms.eba-pibymigp.eu-central-1.elasticbeanstalk.com/'

isRunning = False

def generateIndexer(indexerPath, vendor, category):
    '''
    Download the annoy indexer for the given vendor and the given category
    :param vendor: Vendor
    :param category: Category which Indexer to get
    return: True, '' on success, False, Error description otherwise
    '''
    indexerPath = os.path.join(indexerPath, 'models') #, vendor
    if not os.path.exists(indexerPath):
        os.makedirs(indexerPath)
    #print(indexerPath)

    wrote = 0
    strReq = SERVER_URI + '/annoy-indexer-setup/' + vendor + '/' + category

    strTaskId = None
    try:
        with requests.get(strReq, stream=True) as r:
            blockSize = 1024
            if r.status_code == 200:
                strTaskId = r.json()['id']
    except requests.exceptions.ConnectionError:
        return False, 'Connection Error: Could not generate Task-Id on Server.'

    if strTaskId == None:
        return False, 'Connection Error: Could not get a valid Task Id from Server.'

    result = True
    while result:
        strReq = SERVER_URI + '/get-annoy-indexer/' + strTaskId
        with requests.get(strReq, stream=True) as r:
            blockSize = 1024
            #self.updateProgress.emit(50)
            if r.status_code == 200:
                if 'text/html' in r.headers['content-type']:
                    print(r.json())
                    print(r.json()['result'])
                    if r.json()['result'] != 'running' and r.json()['result'] != 'not started yet':
                        return False, 'Connection Error: Undefined state on Server for current task.'
                    time.sleep(1)
                elif 'application/x-zip-compressed' in r.headers['content-type'] or\
                    'application/zip' in r.headers['content-type']:
                    print(f'r.headers: {r.headers}')
                    total_size = int(r.headers.get('content-length', 0))
                    zipFile = os.path.join(indexerPath, 'ModelFile.zip')
                    if isRunning == False:
                        return False, 'Process aborted by user.'
                    with open(zipFile, 'wb') as f:
                        for data in r.iter_content(blockSize):
                            wrote = wrote + len(data)
                            f.write(data)
                            if isRunning == False:
                                return False, 'Process aborted by user.'
                            #self.updateProgress.emit(float(wrote) * 100 / float(total_size))

                    with zipfile.ZipFile(zipFile, 'r') as zip_ref:
                        zip_ref.extractall(os.path.join(indexerPath, vendor))
                    os.remove(zipFile)

                    strReq = SERVER_URI + '/remove-annoy-indexer/' + strTaskId
                    requests.get(strReq, stream=True)

                    return True, 'Generated valid indexer file.'
                else:
                    return False, 'Error when trying to get content of indexer: %s.' % (r.headers['content-type'])
            else:
                return False, 'Could not generate a valid indexer, code: %s.\nCheck Correct-category name.' % (r.status_code)

    return False, 'Could not start generating indexer.'

def findmatchingPart(lstVisuals):
    parts = {}
    with requests.post(SERVER_URI + '/find-matching-part', json={'data': lstVisuals}) as r:
        if r.status_code == 200:
            parts = r.json()
        if not parts:
            print("No Parts")
        print(parts)

def _writePresetFile(fileName, binaryItems):
    '''
    Write a pickle file exactly as needed in Houdini by writing the byte content into a Preset file
    :param fileName: Pickle file path
    :param itemList: List of elements to write into the file
    :return: None
    '''
    try:
        with open(fileName, 'wb') as fp:
            fp.write(binaryItems)
    except:
        print('Error writing pickle file: %s' % (fileName))

def generatePresetFile(presetFileName, targetPath):
    with requests.post(SERVER_URI + '/get-preset-file', json={'data': presetFileName}) as r:
        if r.status_code == 200:
            content = r.content
            presetFileName = os.path.join(targetPath, presetFileName)
            _writePresetFile(presetFileName, content)
        else:
            return False, 'Error: ' + str(r.status_code)

if __name__ == '__main__':
    # Does not seem to download properly from the server, maybe try if it works locally
    # First variable (Path), can change, the other two MUST be like this
    generateIndexer('E:/blubb', 'Volkswagen', 'LOD_1')

    # Run for each car part
    #findmatchingPart(["0824832b-30da-432c-92cb-39e47fdbf44b_000.071.105.G__TMU_001_002_LTGS_FAHRRADTRAEGER._qJO.fbx.backView.png",
    #                  "0824832b-30da-432c-92cb-39e47fdbf44b_000.071.105.G__TMU_001_002_LTGS_FAHRRADTRAEGER._qJO.fbx.topView.png"])

    # Generate Preset
    #generatePresetFile("039cd16333da29360d2cf8c509b3a142324e0ad0ad5ecd9855797902ea7e2ca2.preset", 'D:/blubb')