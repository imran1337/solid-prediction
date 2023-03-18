from flask import Flask, request, current_app, send_file
from cryptography.fernet import Fernet as F
import boto3
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor
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

threadExecutor = ThreadPoolExecutor(2)
dictUsers = {}
application = Flask(__name__)
# Mongo Global vars
DB_NAME = "test"
COLLECTION_NAME = "JSONInfo"
AMOUNT_PARTS = 7.0

environment = 'dev'


if environment == 'dev':
    import dotenv
    dotenv.load_dotenv()


def mongoConnect(osEnv, dbName, CollectionName):
    mongoURI = os.getenv(osEnv)
    myclient = pymongo.MongoClient(mongoURI)
    mydb = myclient[dbName]
    mycol = mydb[CollectionName]
    return mycol


def mongoReplace(id, errorMessage):
    collection = mongoConnect(
        "MONGODB_URI", "slim-prediction", "featureMapErrors")
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
    bucketName = args[0]
    package = args[1]
    subTaskId = args[2]
    # connect to s3

    failed = False
    while failed == False:
        try:
            s3 = boto3.resource('s3')
            failed = True
        except Exception as error:
            print(error)
            time.sleep(0.5)
            pass
    # download from the specified bucket and key
    result = []
    idx = 0
    featureLen = 0
    for obj in package:
        try:
            awsImage = s3.Object(bucketName, obj)
        except ClientError as e:
            print(e)
            continue
        # Append images to an array. And buffer them.
        awsImageBytes = awsImage.get()['Body'].read()
        arrNumpy = numpy.frombuffer(awsImageBytes, dtype=numpy.float32)
        # print(featureFile, idx, len(featureFiles))
        result.append(arrNumpy)
        if idx == 0:
            featureLen = len(arrNumpy)
            # imagesDict['length'] = len(arrNumpy)
            idx += 1

    return [result, featureLen, subTaskId]


def generateAnnoyIndexerTask(currentPath, vendor, category, generatedUUID):
    # Init AWS
    bucketName = os.getenv("S3_BUCKET")

    # Init MongoDB
    mycol = mongoConnect("MONGODB_URI", DB_NAME, COLLECTION_NAME)
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
        results = executor.map(downloadPackage, args)

        idx = 0
        for obj in results:
            arrayParts += obj[0]
            if idx == 0:
                imagesDict['length'] = obj[1]
            idx += 1

    print(len(arrayParts), len(featureFiles))
    # Create a dataframe and create the indexer
    df = pandas.DataFrame()
    df['features'] = arrayParts

    downloadLocation = currentPath + '/DownloadFiles'
    downloadTotalPath = os.path.join(
        currentPath, downloadLocation, generatedUUID)
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
    # ZIp files
    print('Zip stuff')
    zipLocation = os.path.join(downloadTotalPath, generatedUUID + ".zip")
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
    id = str(uuid.uuid4())
    dictUsers[id] = threadExecutor.submit(
        generateAnnoyIndexerTask, current_app.root_path, vendor, cat, id)
    collection = mongoConnect(
        "MONGODB_URI", "slim-prediction", "AnnoyIndexerErrors")
    collection.insert_one({"id": id, "error": "", "timestamp": datetime.now()})
    return json.dumps({'id': id})


@application.route("/get-annoy-indexer/<id>", methods=['GET'])
def getAnnoyIndexer(id):
    if id not in dictUsers:
        return json.dumps({'id': id, 'result': 'id unknown'})
    if dictUsers[id].running():
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
    mycol = mongoConnect("MONGODB_URI", DB_NAME, COLLECTION_NAME)

    # FInds an object using an array. The "in" keyword is used to search for the array values. It doesn't return duplicates.
    dictToReturn = {}
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
