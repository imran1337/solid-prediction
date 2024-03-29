from flask import Flask, request, current_app, abort, jsonify
from pathlib import Path
from cryptography.fernet import Fernet as F
from concurrent.futures import ThreadPoolExecutor, as_completed
import tqdm
import os
from annoy import AnnoyIndex
import numpy
import pymongo
import pandas
import json
import io
import shutil
import base64
import hashlib
from datetime import datetime, timedelta
from google.cloud import storage
from zipfile import ZipFile, ZipInfo
from google.api_core.exceptions import NotFound
import dotenv
import threading
from threading import Semaphore
from redis import Redis

dotenv.load_dotenv()

environment = os.getenv('FLASK_ENV', 'development')

# Set up Google Cloud Storage client
gcs_client = storage.Client()

# Your Google Cloud Storage bucket name
gcs_bucket_name = os.getenv("GOOGLE_CLOUD_BUCKET_ID")
gcs_bucket = gcs_client.bucket(gcs_bucket_name)

redis_client = Redis.from_url(os.getenv("REDIS_URL"))

# Global variable to track indexing status
INDEXING_LOCK_NAME = "indexing_lock"


# Flask application
application = Flask(__name__)


# Thread executor
thread_executor = ThreadPoolExecutor(max_workers=2)


class Vendor:
    def __init__(self, vendor: str, category: str):
        self.vendor = vendor
        self.category = category

vendor_information = [
    Vendor("Touareg_1", "LOG_1"),
    Vendor("Touareg_2", "LOG_2"),
    Vendor("Touareg_3", "LOG_3"),
    Vendor("Touareg_4", "LOG_4"),
    Vendor("Touareg_5", "LOG_5"),
    Vendor("Touareg_6", "LOG_6"),
    Vendor("Touareg_7", "LOG_7"),
    Vendor("Touareg_8", "LOG_8"),
    Vendor("Touareg_9", "LOG_9"),
    Vendor("Touareg_10", "LOG_10"),
    Vendor("Touareg_11", "LOG_11"),
    Vendor("Touareg_12", "LOG_12"),
    Vendor("Touareg_13", "LOG_13"),
    Vendor("Touareg_14", "LOG_14"),
    Vendor("Touareg_15", "LOG_15"),
    Vendor("Touareg_16", "LOG_16"),
    Vendor("Touareg_17", "LOG_17"),
    Vendor("Touareg_18", "LOG_18"),
    Vendor("Touareg_19", "LOG_19"),
    Vendor("Touareg_20", "LOG_20")
]

# MongoDB connection
def connect_mongo_questions_db():
    mongo_uri = os.getenv("MONGODB_URI")
    my_client = pymongo.MongoClient(mongo_uri)
    return my_client

def get_zip_file_url(identifier: str):
    signed_url = generate_signed_url(f'{identifier}.zip')
    return signed_url

# Initialize default values for dictionary of users
def init_default_value_for_dict_users():
    global dictUsers
    
    # Get folder names from GCS bucket
    try:
        blobs = gcs_bucket.list_blobs()
        folder_names = set()

        for blob in blobs:
            # Extract the folder name from the object's path
            folder_name = blob.name.replace(".zip", '')
            folder_names.add(folder_name)

        # Process folder names and create dictionary entries
        for folder_name in folder_names:
            identifier = folder_name
            dictUsers[identifier] = thread_executor.submit(
                get_zip_file_url, identifier)
    
    except NotFound:
        # Handle the case where the bucket or objects are not found
        print("Bucket or objects not found in GCS.")

# Global variables
dictUsers = {}

mongo_client = connect_mongo_questions_db()

# Mongo Global vars
DB_NAME = "slim-prediction"
# DB_NAME = "slim-prediction-test"
COLLECTION_NAME = "JSONInfo"
AMOUNT_PARTS = 7.0

# Initialize default values
init_default_value_for_dict_users()

def _checkDBSession(clientDB):
    '''
    Check if the current connection is valid
    :return: session object, None otherwise
    '''
    try:
        clientDB.server_info()  # force connection on a request as the
        # connect=True parameter of MongoClient seems
        # to be useless here
    except pymongo.errors.ServerSelectionTimeoutError as err:
        print('Error in Database session connection.')
        return None
    return clientDB

def mongoReplace(id, errorMessage):
    session = _checkDBSession(mongo_client)
    if session:
        collection = session[DB_NAME]["AnnoyIndexerErrors"]
        collection.replace_one(
            {"id": id}, {"id": id, "error": errorMessage, "timestamp": datetime.now()})

def startIndexing(image_data, indexerPath, vendor, category):
    if not os.path.exists(indexerPath):
        os.makedirs(indexerPath)

    # Length of item vector that will be indexed
    f = len(image_data['features'][0])
    t = AnnoyIndex(f, 'euclidean')
    for i, v in tqdm.tqdm(zip(image_data.index, image_data['features'])):
        t.add_item(i, v)
        # print(t, i, v)
    t.build(100)  # 100 trees
    t.save(os.path.join(indexerPath, vendor + '_' + category + '_fvecs.ann'))


def downloadPackage(args):
    bucket_name = args[0]
    package = args[1]
    sub_task_id = args[2]

    def download_single_object(obj):
        try:
            blob = gcs_client.bucket(bucket_name).blob(obj)
            gcs_bytes = blob.download_as_bytes()
            arr_numpy = numpy.frombuffer(gcs_bytes, dtype=numpy.float32)
            return arr_numpy
        except Exception as e:
            print(e)
            return None

    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(download_single_object, obj) for obj in package]
        results = [future.result() for future in as_completed(futures) if future.result() is not None]

    feature_len = len(results[0]) if results else 0

    return [results, feature_len, sub_task_id]

def is_zip_file_exists(object_name:str):
    blob = gcs_bucket.blob(object_name)
    return blob.exists()

def get_remote_zip_file_size(object_name: str):
    try:
        size_in_bytes = gcs_bucket.get_blob(object_name).size
        return size_in_bytes
    except Exception as e:
        print(f"Error getting size for GCS object {object_name}: {e}")
        return None

def upload_zip_to_gcs(object_name, zip_data):
    try:
        blob = gcs_bucket.blob(object_name)
        blob.upload_from_file(zip_data, content_type='application/zip')
    except Exception as e:
        print(f"Error in upload process: {e}")


def generate_signed_url(object_name: str):
    try:
        blob = gcs_bucket.blob(object_name)

        # Fetch expiration_time from environment variable or use a default value (900 seconds)
        expiration_time = int(os.getenv("EXPIRATION_TIME_SECONDS", 900))

        expiration = datetime.utcnow() + timedelta(seconds=expiration_time)

        signed_url = blob.generate_signed_url(
            expiration=expiration,
            version='v4',
            method='GET',
        )

        return signed_url

    except NotFound:
        # Handle the case where the object is not found
        return None

def generateAnnoyIndexerTask(currentPath, vendor, category):
    # Init MongoDB
    session = _checkDBSession(mongo_client)
    if not session:
        return 'DB not reachable', 500

    mycol = session[DB_NAME][COLLECTION_NAME]
    temp = []

    # Check MongoDB to see if the JSON files have a Preset File
    mypresetquery = {"preset_file_name": {"$exists": True},
                  "vendor": vendor, 'category': category}
    mydoc = mycol.find(mypresetquery, {"image_file_names": 1})


    for x in mydoc:
        temp += x["image_file_names"]
    if len(temp) == 0:
        return "Not Acceptable", 406

    imagesDict = {"image_file_names": temp}

    # Get the feature maps of the specific images
    featureFiles = ["featuremap/" +
                    obj.rsplit('.', 1)[0] + ".bin" for obj in temp]
    arrayParts = []
    idx = 0
    fileTuples = []
    count = 100
    for idx in range(0, len(featureFiles), count):
        fileTuples.append((idx, min(idx + count, len(featureFiles))))

    subTaskId = 0

    with ThreadPoolExecutor() as executor:
        args = []
        
        if environment == 'development':
            for fileChunkIdx in fileTuples[:1]:
                args.append([gcs_bucket_name, featureFiles[fileChunkIdx[0]: fileChunkIdx[1]], subTaskId])
                subTaskId += 1
        else:
            for fileChunkIdx in fileTuples:
                args.append([gcs_bucket_name, featureFiles[fileChunkIdx[0]: fileChunkIdx[1]], subTaskId])
                subTaskId += 1


        results = list(executor.map(downloadPackage, args))

        idx = 0
        for obj in results:
            arrayParts += obj[0]
            if idx == 0:
                imagesDict['length'] = obj[1]
            idx += 1

    # Create a dataframe and create the indexer
    df = pandas.DataFrame()
    df['features'] = arrayParts

    downloadLocation = currentPath + '/DownloadFiles'
    downloadTotalPath = os.path.join(
        currentPath, downloadLocation, vendor + ' ' + category)
    print('Start Indexing')

    startIndexing(df, downloadTotalPath, vendor, category)

    indexerPath = os.path.join(
        downloadTotalPath, vendor + '_' + category + '_fvecs.ann')

    # Create JSON File
    json_object = json.dumps(imagesDict, indent=4)
    jsonPath = os.path.join(downloadTotalPath, vendor +
                            '_' + category + "_info.json")

    print('Write output')
    with open(jsonPath, "w") as outfile:
        outfile.write(encryptMessage(os.getenv('ENCRYPTION_KEY'), json_object))

    # Download Both
    # Zip files
    print('Zip stuff')
    
    # Create the zip file in memory
    archive = io.BytesIO()
    with ZipFile(archive, 'w') as zip_archive:
        for file_path in Path(downloadTotalPath).iterdir():
            with open(file_path, 'rb') as file:
                zip_entry_name = file_path.name
                zip_file = ZipInfo(zip_entry_name)
                zip_archive.writestr(zip_file, file.read())

    archive.seek(0)
                    
    # Check if the zip file already exists in GCS
    zip_file_name = f"{vendor}_{category}.zip"
    if is_zip_file_exists(zip_file_name):
        # If it exists, check the size
        remote_zip_size = get_remote_zip_file_size(zip_file_name)

        if remote_zip_size is not None:
            # If the size is available, compare with in-memory size
            in_memory_zip_size = archive.getbuffer().nbytes

            if remote_zip_size == in_memory_zip_size:
                # Skip upload if sizes match
                print("Zip file already exists in GCS with the same size. Skipping upload.")
            else:
                # Upload the new zip file if sizes don't match
                upload_zip_to_gcs(zip_file_name, archive)
        else:
            # Handle the case where size information is not available
            print("Warning: Size information for remote zip file is not available.")
            # Proceed with the upload as a precaution
            upload_zip_to_gcs(zip_file_name, archive)
    else:
        # Upload the zip file if it doesn't exist in GCS
        upload_zip_to_gcs(zip_file_name, archive)

    # Cleanup files
    shutil.rmtree(downloadTotalPath)

    signed_url = generate_signed_url(zip_file_name)

    identifier = f"{vendor}_{category}"

    dictUsers[identifier] = thread_executor.submit(
                signed_url, identifier)

    return signed_url


def generateFernetKey(passcode):
    assert isinstance(passcode, bytes)
    hlib = hashlib.md5()
    hlib.update(passcode)
    return base64.urlsafe_b64encode(hlib.hexdigest().encode('latin-1'))


def encryptMessage(key, msg):
    encoded_key = generateFernetKey(key.encode('utf-8'))
    handler = F(encoded_key)
    encoded_msg = msg if isinstance(msg, bytes) else msg.encode()
    treatment = handler.encrypt(encoded_msg)
    return str(treatment, 'utf-8')


@application.route("/annoy-indexer-setup/<vendor>/<cat>", methods=['GET'])
def annoyIndexer(vendor, cat):
    id = str(vendor + '_' + cat)
    if id not in dictUsers:
        dictUsers[id] = thread_executor.submit(
            generateAnnoyIndexerTask, current_app.root_path, vendor, cat)
    # Init MongoDB
    session = _checkDBSession(mongo_client)
    if not session:
        mongoReplace(id, 'Error getting the DB for Annoy IDX')
    return json.dumps({'id': id})


@application.route("/get-annoy-indexer/<id>", methods=['GET'])
def getAnnoyIndexer(id):
    if id not in dictUsers:
        return json.dumps({'id': id, 'result': 'id unknown'})
    elif dictUsers[id].running():
        return json.dumps({'id': id, 'result': 'running'})
    elif dictUsers[id].cancelled():
        return json.dumps({'id': id, 'result': 'cancelled'})
    elif dictUsers[id].done():
        object_name = f'{id}.zip'
        signed_url = generate_signed_url(object_name)
        if signed_url is not None:
            return json.dumps({'id': id, 'result': 'done', 'fileUrl': signed_url})
        else:
            # Return a 404 response for file not found
            abort(404, 'File not found')
    else:
        return json.dumps({'id': id, 'result': 'not started yet'})


@application.route("/", methods=['GET'])
def index():
    return 'Running'

# Acquire the non-blocking lock
def acquire_indexing_lock():
    return redis_client.setnx(INDEXING_LOCK_NAME, 1)

# Release the lock
def release_indexing_lock():
    redis_client.delete(INDEXING_LOCK_NAME)

# Semaphore for synchronization
semaphore = Semaphore()

@application.route("/process", methods=['GET'])
def process():

     # Acquire the distributed lock
    lock_acquired = acquire_indexing_lock()

    print(f'===lock_acquired=== {lock_acquired}')

    if not lock_acquired:
        return jsonify({'status': False, 'msg': 'Indexing process is already in progress.'})


    try:
        # Start the background task using a separate thread
        threading.Thread(target=start_indexing_process).start()

        print("Initiating indexing process...")

        return jsonify({'status': True, 'msg': 'Indexing process has been initiated.'})

    except Exception as error:
        release_indexing_lock()
        print(f'Error in process: {error}')
        return jsonify({'status': False, 'msg': 'Error in Indexing Process'})


def annoyIndexerJob(vendor, cat):
    try:
        with application.app_context():
            id = str(vendor + '_' + cat)
            try:
                generateAnnoyIndexerTask(current_app.root_path, vendor, cat)
                # Init MongoDB
                session = _checkDBSession(mongo_client)
                if not session:
                    mongoReplace(id, 'Error getting the DB for Annoy IDX')
            except Exception as e:
                print(f"An error occurred: {str(e)}")
                mongoReplace(id, f'An error occurred: {str(e)}')
            
            return json.dumps({'id': id})
    except Exception as err:
        print(f"===Error in annoyIndexerJob: === {err}")


def start_indexing_process():    
    try:
        # Start the indexing process for each vendor_info sequentially
        for vendor_info in vendor_information:
            vendor = vendor_info.vendor
            category = vendor_info.category
            print(f'Start task to generate indexer for {vendor} - {category}')

            # Acquire the semaphore before starting the Annoy indexer task
            semaphore.acquire()

            try:
                result = annoyIndexerJob(vendor, category)

                if result:
                    print(f'Task for {vendor} - {category} completed successfully')
                else:
                    print(f'Task for {vendor} - {category} failed')

            except Exception as error:
                print(f'Error in annoyIndexerJob: {error}')

            finally:
                # Release the semaphore after completing the Annoy indexer task or in case of an exception
                semaphore.release()

    except Exception as error:
        print(f'Error in start_indexing_process: {error}')

    finally:
        release_indexing_lock()



@application.route("/find-matching-part", methods=['POST'])
def findMatchingPart():
    content = request.json
    session = _checkDBSession(mongo_client)
    dictToReturn = {}
    if session:
        mycol = session[DB_NAME][COLLECTION_NAME]
        # FInds an object using an array. The "in" keyword is used to search for the array values. It doesn't return duplicates.
        amount = 0
        try:
            for obj in content["data"]:
                res = mycol.find_one({"image_file_names": obj}, {"_id": 0})
                id = res['universal_uuid'] + res['file_name']
                if id not in dictToReturn:
                    res['histo'] = 1
                    dictToReturn[id] = res
                else:
                    dictToReturn[id]['histo'] += 1
                amount += 1
        except Exception as error:
            mongoReplace(id, "Failed to find matching part.")
            print(error)
        try:
            for obj in dictToReturn:
                dictToReturn[obj]['histo'] = float(
                    dictToReturn[obj]['histo']) / AMOUNT_PARTS
        except Exception as error:
            mongoReplace(id, "Failed to find matching part.")
            print(error)

    return json.dumps(dictToReturn)


@application.route("/get-preset-file", methods=['POST'])
def getPresetFile():
    content = request.json
    request_preset_file = content["data"]
    request_preset_file_key = f"preset/{request_preset_file}"

    try:
        blob = gcs_client.bucket(gcs_bucket_name).blob(request_preset_file_key)
        gcs_preset_bytes = blob.download_as_bytes()
    except Exception as error:
        print(error)

    return gcs_preset_bytes


@application.route("/get-img-file", methods=['POST'])
def getImageFile():
    content = request.json
    request_image_file = content["data"]
    request_image_file_key = f"img/{request_image_file}"

    try:
        blob = gcs_client.bucket(gcs_bucket_name).blob(request_image_file_key)
        gcs_image_bytes = blob.download_as_bytes()
    except Exception as error:
        print(error)

    return gcs_image_bytes



@application.route("/is-alive", methods=['GET'])
def isAlive():
    return json.dumps({'alive': 1})

@application.route("/reset-redis", methods=['GET'])
def resetRedis():
    try:
        # Delete the indexing lock in Redis
        redis_client.delete(INDEXING_LOCK_NAME)
        return jsonify({'status': True, 'msg': 'Redis reset successful.'})
    except Exception as error:
        return jsonify({'status': False, 'msg': f'Error resetting Redis: {str(error)}'})


if __name__ == '__main__':
    application.run(port=int(os.environ.get("PORT", 8080)),host='0.0.0.0',debug=True, threaded=True)
