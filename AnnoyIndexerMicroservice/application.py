from flask import Flask, request, current_app, redirect, abort
from pathlib import Path
from cryptography.fernet import Fernet as F
import boto3
from botocore.exceptions import ClientError
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

environment = 'dev'

if environment == 'dev':
    try:
        import dotenv
        dotenv.load_dotenv()
    except ImportError:
        pass

# Set up Google Cloud Storage client
gcs_client = storage.Client()

# Your Google Cloud Storage bucket name
gcs_bucket_name = os.getenv("GOOGLE_CLOUD_BUCKET_ID")
gcs_bucket = gcs_client.bucket(gcs_bucket_name)

# Flask application
application = Flask(__name__)

# Thread executor
thread_executor = ThreadPoolExecutor(max_workers=2)

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
dictUsersForJobs = {}
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

try:
    s3 = boto3.resource('s3')
except Exception as error:
    print(error)

def downloadPackage(args):
    bucketName = args[0]
    package = args[1]
    subTaskId = args[2]

    def download_single_object(obj):
        try:
            awsImage = s3.Object(bucketName, obj)
            awsImageBytes = awsImage.get()['Body'].read()
            arrNumpy = numpy.frombuffer(awsImageBytes, dtype=numpy.float32)
            return arrNumpy
        except ClientError as e:
            print(e)
            return None

    # Use ThreadPoolExecutor to parallelize downloads
    with ThreadPoolExecutor() as executor:
        # Use as_completed to iterate over completed futures
        futures = [executor.submit(download_single_object, obj) for obj in package]
        results = [future.result() for future in as_completed(futures) if future.result() is not None]

    # Calculate the feature length
    featureLen = len(results[0]) if results else 0

    return [results, featureLen, subTaskId]

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
    blob = gcs_bucket.blob(object_name)
    blob.upload_from_file(zip_data, content_type='application/zip')

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
    # Init AWS
    bucketName = os.getenv("S3_BUCKET")

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
        for fileChunkIdx in fileTuples[:1]:
            args.append(
                [bucketName, featureFiles[fileChunkIdx[0]: fileChunkIdx[1]], subTaskId])
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


@application.route("/job/annoy-indexer-setup/<vendor>/<cat>", methods=['GET'])
def annoyIndexerJob(vendor, cat):
    id = str(vendor + '_' + cat)
    dictUsersForJobs[id] = thread_executor.submit(
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

@application.route("/job/get-annoy-indexer/<id>", methods=['GET'])
def getAnnoyIndexerJob(id):
    if id not in dictUsersForJobs:
        return json.dumps({'id': id, 'result': 'id unknown'})
    elif dictUsersForJobs[id].running():
        return json.dumps({'id': id, 'result': 'running'})
    elif dictUsersForJobs[id].cancelled():
        return json.dumps({'id': id, 'result': 'cancelled'})
    elif dictUsersForJobs[id].done():
        # Update dictUsers when job is completed
        dictUsers[id] = dictUsersForJobs[id]
        gcs_zip_url = dictUsersForJobs[id].result()
        if gcs_zip_url is not None:
            return json.dumps({'id': id, 'result': 'done', 'fileUrl': gcs_zip_url})
        else:
            # Return a 404 response for file not found
            abort(404, 'File not found')
    else:
        return json.dumps({'id': id, 'result': 'not started yet'})


@application.route("/", methods=['GET'])
def index():
    return 'Running'


@application.route("/remove-annoy-indexer/<id>", methods=['GET'])
def removeAnnoyIndexer(id):
    try:
        if id in dictUsers and dictUsers[id].done():
            # Get the GCS object name based on the id
            object_name = f"{id}.zip"

            # Check if the GCS object exists before trying to delete it
            blob = gcs_bucket.blob(object_name)
            if blob.exists():
                # Delete the object from the GCS bucket
                blob.delete()

            # Remove the entry from the dictionary
            del dictUsers[id]

            return json.dumps({'id': id, 'result': 'removed'})
        else:
            # Handle the case where id is not found in dictUsers or dictUsers[id] is not done
            return json.dumps({'id': id, 'result': 'id not found or not done'})
    except Exception as error:
        mongoReplace(id, "Failed to remove annoy indexer.")
        print(error)
        return json.dumps({'id': id, 'result': 'error'})


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
    requestPresetFile = content["data"]
    requestPresetFileKey = "preset/" + requestPresetFile
    try:
        s3 = boto3.resource('s3')
        bucketName = os.getenv("S3_BUCKET")
    except Exception as error:
        print(error)
    awsPresetFile = s3.Object(bucketName, requestPresetFileKey)
    try:
        awsPresetBytes = awsPresetFile.get()['Body'].read()
    except Exception as error:
        print(error)
    return awsPresetBytes


@application.route("/get-img-file", methods=['POST'])
def getImageFile():
    content = request.json
    requestImageFile = content["data"]
    requestImageFileKey = "img/" + requestImageFile

    try:
        s3 = boto3.resource('s3')
        bucketName = os.getenv("S3_BUCKET")
    except Exception as error:
        print(error)
    awsPresetFile = s3.Object(bucketName, requestImageFileKey)
    try:
        awsPresetBytes = awsPresetFile.get()['Body'].read()
    except Exception as error:
        print(error)

    return awsPresetBytes


@application.route("/is-alive", methods=['GET'])
def isAlive():
    return json.dumps({'alive': 1})


if __name__ == '__main__':
    application.run(debug=True, threaded=True)
