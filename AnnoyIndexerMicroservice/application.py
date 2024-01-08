from flask import Flask, request, current_app, send_file
from cryptography.fernet import Fernet as F
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
import shutil
import base64
import hashlib
from datetime import datetime

environment = 'dev'

if environment == 'dev':
    try:
        import dotenv
        dotenv.load_dotenv()
    except ImportError:
        pass

# Flask application
application = Flask(__name__)

# Thread executor
thread_executor = ThreadPoolExecutor(max_workers=2)

# MongoDB connection
def connect_mongo_questions_db():
    mongo_uri = os.getenv("MONGODB_URI")
    my_client = pymongo.MongoClient(mongo_uri)
    return my_client

# Split folder names
def split_folder_names(download_folder_path):
    if not os.path.exists(download_folder_path) or not os.path.isdir(download_folder_path):
        return []

    folder_names = [folder for folder in os.listdir(download_folder_path) if os.path.isdir(os.path.join(download_folder_path, folder))]

    result_list = []
    for folder_name in folder_names:
        parts = folder_name.split(" ", 1)
        if len(parts) == 2:
            result_list.append({'vendor': parts[0], 'category': parts[1]})
        else:
            result_list.append({'vendor': folder_name, 'category': ''})

    return result_list

# Get zip file path
def get_zip_file_path(download_folder_path: str, folder_name: str, identifier: str):
    zip_location = os.path.join(download_folder_path, folder_name, f"{identifier}.zip")
    return zip_location

# Initialize default values for dictionary of users
def init_default_value_for_dict_users():
    global dictUsers
    download_folder_path = os.path.join(application.root_path, 'DownloadFiles')
    result = split_folder_names(download_folder_path)

    for item in result:
        identifier = f"{item['vendor']}_{item['category']}"
        folder_name = f"{item['vendor']} {item['category']}"
        dictUsers[identifier] = thread_executor.submit(
            get_zip_file_path, download_folder_path, folder_name, identifier)

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
        for fileChunkIdx in fileTuples:
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
    zipLocation = os.path.join(downloadTotalPath, vendor + '_' + category + ".zip")

    with zipfile.ZipFile(zipLocation, 'w', zipfile.ZIP_DEFLATED) as zipObj:
        zipObj.write(jsonPath, os.path.basename(jsonPath))
        zipObj.write(indexerPath, os.path.basename(indexerPath))

    # Cleanup files
    os.remove(jsonPath)
    os.remove(indexerPath)

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
        return send_file(dictUsers[id].result(), as_attachment=True)
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
        return send_file(dictUsersForJobs[id].result(), as_attachment=True)
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
