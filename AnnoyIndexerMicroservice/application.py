import sys
from flask import Flask, request, current_app, send_file
import boto3
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
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
from io import BytesIO

application = Flask(__name__)

def mongoConnect(osEnv, dbName, CollectionName):
    mongoURI = os.getenv(osEnv)
    myclient = pymongo.MongoClient(mongoURI)
    mydb = myclient[dbName]
    mycol = mydb[CollectionName]
    return mycol

# Mongo Global vars
DB_NAME = "test"
COLLECTION_NAME = "JSONInfo"

def start_indexing(image_data, indexerPath, vendor):
    if not os.path.exists(indexerPath):
        os.makedirs(indexerPath)

    # Length of item vector that will be indexed
    f = len(image_data['features'][0])
    t = AnnoyIndex(f, 'euclidean')
    for i, v in tqdm.tqdm(zip(image_data.index, image_data['features'])):
        t.add_item(i, v)
        # print(t, i, v)
    t.build(100)  # 100 trees
    t.save(os.path.join(indexerPath, vendor + '_fvecs.ann'))

@application.route("/", methods=['GET'])
def helloWorld():
    return "Hello World"

def downloadPackage(args):
    bucketName = args[0]
    package = args[1]
    subTaskId = args[2]
    #connect to s3

    failed = False
    while failed == False:
        try:
            s3 = boto3.resource('s3')
            failed = True
        except Exception as error:
            print(error)
            time.sleep(0.5)
            pass
    #download from the specified bucket and key
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
        #print(featureFile, idx, len(featureFiles))
        result.append(arrNumpy)
        if idx == 0:
            featureLen = len (arrNumpy)
            #imagesDict['length'] = len(arrNumpy)
            idx += 1

    return [result, featureLen, subTaskId]

@application.route("/annoy-indexer", methods=['GET'])
def AnnoyIndexer():

    #import dotenv
    #dotenv.load_dotenv()

    currentPath = current_app.root_path
    # test = os.listdir(currentPath)

    # TODO get a better way to clean up the files
    # for item in test:
    #   if item.endswith(".zip"):
    #      os.remove(os.path.join(currentPath, item))

    generatedUUID = str(uuid.uuid4())

    vendor = "Volkswagen"

    # Init AWS
    bucketName = os.getenv("S3_BUCKET")

    # Init MongoDB
    mycol = mongoConnect("MONGODB_URI", DB_NAME, COLLECTION_NAME)
    temp = []

    # Check MongoDB to see if the JSON files have a PSF File
    mypsfquery = {"psf_file_name": {"$exists": True}, "vendor": vendor}
    mydoc = mycol.find(mypsfquery, {"image_file_names": 1})

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
            args.append([bucketName, featureFiles[fileChunkIdx[0]: fileChunkIdx[1]], subTaskId])
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

    downloadLocation = os.path.dirname(__file__)
    if getattr(sys, 'frozen', False):
        downloadLocation = os.path.dirname(sys.executable)
    downloadLocation += '/DownloadFiles'
    downloadTotalPath = os.path.join(
        currentPath, downloadLocation, generatedUUID)
    print('Start Indexing')
    start_indexing(df, downloadTotalPath, vendor)
    indexerPath = os.path.join(downloadTotalPath, vendor + '_fvecs.ann')

    # Create JSON File
    json_object = json.dumps(imagesDict, indent=4)
    jsonPath = os.path.join(downloadTotalPath, "info.json")

    print('Write output')
    with open(jsonPath, "w") as outfile:
        outfile.write(json_object)

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
    # TODO: Build a function that cleans up the whole directory after a day

    return send_file(zipLocation, as_attachment=True)

@application.route("/find-matching-part", methods=['POST'])
def FindMatchingPart():
    content = request.json
    mycol = mongoConnect("MONGODB_URI", DB_NAME, COLLECTION_NAME)

    # FInds an object using an array. The "in" keyword is used to search for the array values. It doesn't return duplicates.
    dictToReturn = {}
    amount = 0
    for obj in content["data"]:
        res = mycol.find_one({"image_file_names": obj}, {"_id": 0})
        id = res['universal_uuid'] + res['file_name']
        if id not in dictToReturn:
            res['histo'] = 1
            dictToReturn[id] = res
        else:
            dictToReturn[id]['histo'] += 1
        amount += 1

    for obj in dictToReturn:
        dictToReturn[obj]['histo'] = float(dictToReturn[obj]['histo']) / float(amount)

    return json.dumps(dictToReturn)

@application.route("/get-psf-file", methods=['POST'])
def GetPsfFile():
    content = request.json
    requestPsfFile = content["data"]
    requestPsfFileKey = "psf/" + requestPsfFile

    s3 = boto3.resource('s3')
    bucketName = os.getenv("S3_BUCKET")
    awsPsfFile = s3.Object(bucketName, requestPsfFileKey)
    print(awsPsfFile)
    awsPsfBytes = awsPsfFile.get()['Body'].read()

    return awsPsfBytes

@application.route("/get-img-file", methods=['POST'])
def GetImageFile():
    content = request.json
    requestImageFile = content["data"]
    requestImageFileKey = "img/" + requestImageFile

    s3 = boto3.resource('s3')
    bucketName = os.getenv("S3_BUCKET")
    awsPsfFile = s3.Object(bucketName, requestImageFileKey)
    print(awsPsfFile)
    awsPsfBytes = awsPsfFile.get()['Body'].read()

    return awsPsfBytes

if __name__ == '__main__':
    application.run(debug=True, threaded=True)
