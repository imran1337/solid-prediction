import sys
from flask import Flask, request, current_app, send_file
import boto3
from botocore.exceptions import ClientError
import tqdm
import os
from annoy import AnnoyIndex
import numpy
import pymongo
import pandas
import json
import zipfile
import uuid

application = Flask(__name__)

# load_dotenv()


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
    s3 = boto3.resource('s3')
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
    for featureFile in featureFiles:
        # Check if the object exists or if we need to error handle it
        try:
            awsImage = s3.Object(bucketName, featureFile)
            # print(awsImage)
        except ClientError:
            print(featureFile)
            continue
        # Append images to an array. And buffer them.
        awsImageBytes = awsImage.get()['Body'].read()
        arrNumpy = numpy.frombuffer(awsImageBytes, dtype=numpy.float32)
        print(featureFile, idx, len(featureFiles))
        arrayParts.append(arrNumpy)
        if idx == 0:
            imagesDict['length'] = len(arrNumpy)
        idx += 1
        if idx > 100:
            break

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
    result = mycol.find(
        {"image_file_names": {"$in": content["data"]}}, {"_id": 0})
    listToReturn = [x for x in result]

    return json.dumps(listToReturn)


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


if __name__ == '__main__':
    application.run(debug=True, threaded=True)
