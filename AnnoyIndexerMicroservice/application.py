from flask import Flask, request, current_app, send_file
from cryptography.fernet import Fernet as F
from botocore.client import Config
import boto3
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import tqdm
import os
from annoy import AnnoyIndex
import numpy
import pymongo
import pandas
import json
import zipfile
import uuid
import shutil
import base64
import hashlib
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(filename='s3_debug.log', level=logging.DEBUG)

# Enable logging for boto3
logger = logging.getLogger('boto3')
logger.setLevel(logging.DEBUG)

# Enable logging for botocore
logger = logging.getLogger('botocore')
logger.setLevel(logging.DEBUG)


environment = 'dev'

if environment == 'dev':
    try:
        import dotenv
        dotenv.load_dotenv()
    except:
        pass


def connectMongoQuestionsDB():
    mongoURI = os.getenv("MONGODB_URI")
    print("MONGODB_URI:", mongoURI)
    myclient = pymongo.MongoClient(mongoURI)
    return myclient

threadExecutor = ThreadPoolExecutor()
dictUsers = {}
application = Flask(__name__)
mongoClient = connectMongoQuestionsDB()
# Mongo Global vars
DB_NAME = "slim-prediction"
# DB_NAME = "slim-prediction-test"
COLLECTION_NAME = "JSONInfo"
AMOUNT_PARTS = 7.0

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
    session = _checkDBSession(mongoClient)
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


# s3 = boto3.resource('s3', config=Config(signature_version='s3v4', s3={'use_accelerate_endpoint': True}))

s3 = boto3.resource('s3')

def downloadPackage(args):
    bucketName = args[0]
    package = args[1]
    subTaskId = args[2]

    # Connect to S3
    s3 = boto3.resource('s3')

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

def generateAnnoyIndexerTask(currentPath, vendor, category, generatedUUID):
    start_time = time.time()

    # Init AWS
    bucketName = os.getenv("S3_BUCKET")

    # Init MongoDB
    session = _checkDBSession(mongoClient)
    if not session:
        elapsed_time = time.time() - start_time
        print(f"Fetching data time: {elapsed_time} seconds")
        return 'DB not reachable', 500

    mycol = session[DB_NAME][COLLECTION_NAME]
    temp = []

    # Check MongoDB to see if the JSON files have a Preset File
    mongo_start_time = time.time()
    mypresetquery = {"preset_file_name": {"$exists": True},
                  "vendor": vendor, 'category': category}
    mydoc = mycol.find(mypresetquery, {"image_file_names": 1})
    mongo_elapsed_time = time.time() - mongo_start_time

    print(f"mydoc: {mydoc}")

    for x in mydoc:
        temp += x["image_file_names"]
    if len(temp) == 0:
        elapsed_time = time.time() - start_time
        print(f"Fetching data time: {elapsed_time} seconds")
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

    # Log or print the count of items in fileTuples and featureFiles
    print(f"fileTuples count: {len(fileTuples)}")
    print(f"featureFiles count: {len(featureFiles)}")

    # Log or print fileTuples
    print("fileTuples:")
    print(json.dumps(fileTuples, indent=2))

    # Log or print featureFiles
    print("featureFiles:")
    print(json.dumps(featureFiles, indent=2))

    subTaskId = 0
    total_requests_sent = 0  # Variable to track the total number of requests sent

    with ThreadPoolExecutor() as executor:
        args = []
        for fileChunkIdx in fileTuples:
            args.append(
                [bucketName, featureFiles[fileChunkIdx[0]: fileChunkIdx[1]], subTaskId])
            subTaskId += 1

        download_start_time = time.time()
        results = list(executor.map(downloadPackage, args))
        download_elapsed_time = time.time() - download_start_time

        idx = 0
        for obj in results:
            arrayParts += obj[0]
            if idx == 0:
                imagesDict['length'] = obj[1]
            idx += 1

            # Increment the total number of requests sent
            total_requests_sent += len(args[idx-1][1])

    print(len(arrayParts), len(featureFiles))

    print(f"arrayParts count: {len(arrayParts)}")


    # Create a dataframe and create the indexer
    df_start_time = time.time()
    df = pandas.DataFrame()
    df['features'] = arrayParts
    df_elapsed_time = time.time() - df_start_time

    downloadLocation = currentPath + '/DownloadFiles'
    downloadTotalPath = os.path.join(
        currentPath, downloadLocation, generatedUUID)
    print('Start Indexing')

    indexing_start_time = time.time()
    startIndexing(df, downloadTotalPath, vendor, category)
    indexing_elapsed_time = time.time() - indexing_start_time

    indexerPath = os.path.join(
        downloadTotalPath, vendor + '_' + category + '_fvecs.ann')

    # Create JSON File
    json_object = json.dumps(imagesDict, indent=4)
    jsonPath = os.path.join(downloadTotalPath, vendor +
                            '_' + category + "_info.json")

    print('Write output')
    encryption_start_time = time.time()
    with open(jsonPath, "w") as outfile:
        outfile.write(encryptMessage(os.getenv('ENCRYPTION_KEY'), json_object))
    encryption_elapsed_time = time.time() - encryption_start_time

    # Download Both
    # Zip files
    print('Zip stuff')
    zipLocation = os.path.join(downloadTotalPath, generatedUUID + ".zip")

    zip_start_time = time.time()
    with zipfile.ZipFile(zipLocation, 'w', zipfile.ZIP_DEFLATED) as zipObj:
        zipObj.write(jsonPath, os.path.basename(jsonPath))
        zipObj.write(indexerPath, os.path.basename(indexerPath))
    zip_elapsed_time = time.time() - zip_start_time

    # Cleanup files
    cleanup_start_time = time.time()
    os.remove(jsonPath)
    os.remove(indexerPath)
    cleanup_elapsed_time = time.time() - cleanup_start_time

    elapsed_time = time.time() - start_time
    print(f"Total time for generateAnnoyIndexerTask: {elapsed_time} seconds")
    print(f"Total requests sent: {total_requests_sent}")
    print(f"MongoDB time: {mongo_elapsed_time} seconds")
    print(f"Download time: {download_elapsed_time} seconds")
    print(f"DataFrame time: {df_elapsed_time} seconds")
    print(f"Indexing time: {indexing_elapsed_time} seconds")
    print(f"Encryption time: {encryption_elapsed_time} seconds")
    print(f"Zip time: {zip_elapsed_time} seconds")
    print(f"Cleanup time: {cleanup_elapsed_time} seconds")

    return zipLocation



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
    id = str(uuid.uuid4())
    dictUsers[id] = threadExecutor.submit(
        generateAnnoyIndexerTask, current_app.root_path, vendor, cat, id)
    # Init MongoDB
    session = _checkDBSession(mongoClient)
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
        return send_file(dictUsers[id].result(), as_attachment=True)
    else:
        return json.dumps({'id': id, 'result': 'not started yet'})


@application.route("/", methods=['GET'])
def index():
    return 'Running'


@application.route("/remove-annoy-indexer/<id>", methods=['GET'])
def removeAnnoyIndexer(id):
    try:
        if id in dictUsers and dictUsers[id].done():
            shutil.rmtree(os.path.join(
                current_app.root_path, 'DownloadFiles', id))
            del (dictUsers[id])
            return json.dumps({'id': id, 'result': 'removed'})
    except Exception as error:
        mongoReplace(id, "Failed to remove annoy indexer.")
        print(error)
        return json.dumps({'id': id, 'result': 'id unknown'})


@application.route("/find-matching-part", methods=['POST'])
def findMatchingPart():
    content = request.json
    session = _checkDBSession(mongoClient)
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
