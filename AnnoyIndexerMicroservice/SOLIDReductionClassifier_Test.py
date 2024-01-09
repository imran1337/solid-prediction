import os
import requests
import time
from tqdm import tqdm
from urllib.parse import unquote
from urllib.parse import urlparse, unquote

# URL for getting the annoy indexer from
SERVER_URI = 'http://127.0.0.1:5000'
UPLOAD_SERVER_URI = 'http://upload-files-ms.eba-rvniqqiy.eu-central-1.elasticbeanstalk.com/'
FM_SERVER_URI = 'http://slim-feature-map-ms.eba-pibymigp.eu-central-1.elasticbeanstalk.com/'

def download_file_from_signed_url(signed_url, destination_dir):
    """Download a file from a signed URL with a progress bar."""
    with requests.get(signed_url, stream=True) as r:
        try:
            r.raise_for_status()
        except requests.exceptions.RequestException as e:
            return False, f'Request Error: {e}'

        # Extract the filename from the URL path
        url_path = urlparse(r.url).path
        filename = os.path.basename(unquote(url_path))
        destination_path = os.path.join(destination_dir, filename)

        total_size = int(r.headers.get('content-length', 0))
        block_size = 1024

        with open(destination_path, 'wb') as f, tqdm(
            desc="Downloading",
            total=total_size,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
        ) as pbar:
            for chunk in r.iter_content(chunk_size=block_size):
                f.write(chunk)
                pbar.update(len(chunk))
                time.sleep(0.1)

        return True, destination_path

isRunning = False

def generateIndexer(indexerPath, vendor, category):
    '''
    Download the annoy indexer for the given vendor and the given category
    :param vendor: Vendor
    :param category: Category which Indexer to get
    return: True, '' on success, False, Error description otherwise
    '''
    indexerPath = os.path.join(indexerPath, 'models')  #, vendor
    if not os.path.exists(indexerPath):
        os.makedirs(indexerPath)

    wrote = 0
    strReq = SERVER_URI + '/annoy-indexer-setup/' + vendor + '/' + category

    strTaskId = None
    try:
        with requests.get(strReq, stream=True) as r:
            r.raise_for_status()
            if r.status_code == 200:
                strTaskId = r.json().get('id')
    except requests.exceptions.RequestException as e:
        return False, f'Request Error: {e}'

    if strTaskId is None:
        return False, 'Connection Error: Could not get a valid Task Id from Server.'

    result = True
    while result:
        strReq = SERVER_URI + '/get-annoy-indexer/' + strTaskId
        with requests.get(strReq, stream=True) as r:
            try:
                r.raise_for_status()
                result = r.json().get('result')
            except requests.exceptions.RequestException as e:
                return False, f'Request Error: {e}'

            if result == 'running' or result == 'not started yet':
                time.sleep(1)
            elif result == 'cancelled':
                return False, 'Process cancelled by the server.'
            elif result == 'done':
                fileUrl = r.json().get('fileUrl')
                # Download and save the file
                download_path = os.path.join(indexerPath, vendor, category)
                success, download_result = download_file_from_signed_url(fileUrl, download_path)

                if success:
                    return True, f'Generated valid indexer file. Downloaded to: {download_result}'
                else:
                    return False, f'Error during file download: {download_result}'
            else:
                return False, 'Undefined state on Server for the current task.'

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