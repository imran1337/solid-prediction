import schedule
import time
import requests
import os

# URL for getting the annoy indexer from
SERVER_URI = 'http://127.0.0.1:5000'

class Vendor:
    def __init__(self, vendor: str, category: str):
        self.vendor = vendor
        self.category = category

vendor_information = [
    Vendor("Volkswagen", "LOD_1"),
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
                result = r.json()['result']

                if result == 'running' or result == 'not started yet':
                    time.sleep(1)
                elif result == 'cancelled':
                    return False, 'Process cancelled by the server.'
                elif result == 'done':
                    return True, 'Generated valid indexer file.'
                else:
                    return False, 'Undefined state on Server for the current task.'
            else:
                return False, 'Could not generate a valid indexer, code: %s.' % (r.status_code)

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

# Fetch the schedule interval from environment variables (default to 12 hours / 43200 seconds)
schedule_interval = int(os.getenv("SCHEDULE_INTERVAL_SECONDS", 43200))

# Schedule the job with the specified interval
schedule.every(schedule_interval).seconds.do(job)

while 1:
    schedule.run_pending()
    time.sleep(1)