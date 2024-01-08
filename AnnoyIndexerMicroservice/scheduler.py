import schedule
import time
import time
import requests

# URL for getting the annoy indexer from
SERVER_URI = 'http://127.0.0.1:5000'

class Vendor:
    def __init__(self, vendor: str, category: str):
        self.vendor = vendor
        self.category = category

vendor_information = [
    Vendor("Volkswagen", "LOD_1"),
    # Vendor("Volkswagen", "LOD_9")
]

isRunning = False


def generateIndexer(vendor, category):
    '''
    Download the annoy indexer for the given vendor and the given category
    :param vendor: Vendor
    :param category: Category which Indexer to get
    return: True, '' on success, False, Error description otherwise
    '''
   
    strReq = SERVER_URI + '/job/annoy-indexer-setup/' + vendor + '/' + category

    strTaskId = None
    try:
        with requests.get(strReq, stream=True) as r:
            if r.status_code == 200:
                strTaskId = r.json()['id']
    except requests.exceptions.ConnectionError:
        return False, 'Connection Error: Could not generate Task-Id on Server.'

    if strTaskId is None:
        return False, 'Connection Error: Could not get a valid Task Id from Server.'

    while True:
        strReq = SERVER_URI + '/job/get-annoy-indexer/' + strTaskId
        with requests.get(strReq, stream=True) as r:
            if r.status_code == 200:
                if 'text/html' in r.headers['content-type']:
                    if r.json()['result'] != 'running' and r.json()['result'] != 'not started yet':
                        return False, 'Connection Error: Undefined state on Server for current task.'
                    time.sleep(1)
                elif 'application/x-zip-compressed' in r.headers['content-type'] or 'application/zip' in r.headers['content-type']:
                    return True, 'Generated valid indexer file.'
                else:
                    return False, 'Error when trying to get content of indexer: %s.' % (r.headers['content-type'])
            else:
                return False, 'Could not generate a valid indexer, code: %s.\nCheck Correct-category name.' % (r.status_code)

def job():
    global isRunning
    if not isRunning:
        isRunning = True
        for vendor_info in vendor_information:
            vendor = vendor_info.vendor
            category = vendor_info.category
            print(f"Start task to generate indexer for {vendor} - {category}")
            result, message = generateIndexer(vendor, category)
            if result:
                print(f"Successfully generated indexer for {vendor} - {category}")
            else:
                print(f"Error generating indexer for {vendor} - {category}: {message}")
        isRunning = False

schedule.every(10).seconds.do(job)
 # schedule.every().day.at("00:00").do(job)

while 1:
    schedule.run_pending()
    time.sleep(10)