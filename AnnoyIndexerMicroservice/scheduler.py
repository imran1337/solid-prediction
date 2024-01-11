import schedule
import time
import requests
import os
import dotenv

dotenv.load_dotenv()

# URL for getting the annoy indexer from
SERVER_URI = os.getenv('SERVER_URL', 'http://127.0.0.1:5000')

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
    max_retries = 5
    retry_count = 0

    while retry_count < max_retries:
        try:
            with requests.get(strReq, stream=True) as r:
                r.raise_for_status()
                if r.status_code == 200:
                    strTaskId = r.json().get('id')
                    break  # Break out of the loop if successful
        except requests.exceptions.ConnectionError as e:
            retry_count += 1
            if retry_count == max_retries:
                return False, f'Connection Error: {e} (Max retries reached)'
            time.sleep(1)
        except requests.exceptions.RequestException as e:
            return False, f'Request Error: {e}'
        except ValueError as e:
            return False, f'Error decoding JSON: {e}'

    if strTaskId is None:
        return False, 'Connection Error: Could not get a valid Task Id from Server.'

    while True:
        strReq = SERVER_URI + '/job/get-annoy-indexer/' + strTaskId
        with requests.get(strReq, stream=True) as r:
            try:
                r.raise_for_status()
                result = r.json().get('result')
            except requests.exceptions.RequestException as e:
                return False, f'Request Error: {e}'
            except ValueError as e:
                return False, f'Error decoding JSON: {e}'

            if result == 'running' or result == 'not started yet':
                time.sleep(1)
            elif result == 'cancelled':
                return False, 'Process cancelled by the server.'
            elif result == 'done':
                return True, 'Generated valid indexer file.'
            else:
                return False, 'Undefined state on Server for the current task.'

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
schedule_interval = int(os.getenv("SCHEDULE_INTERVAL_SECONDS", 10))

print(f'===SCHEDULE_INTERVAL_SECONDS=== {schedule_interval}')

# Schedule the job with the specified interval
schedule.every(schedule_interval).seconds.do(job)

while 1:
    schedule.run_pending()
    time.sleep(1)
