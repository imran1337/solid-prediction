from flask import Flask, request
import boto3
import os
import io
import pymongo
import time
import uuid
import json
from featureMap import Index
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

# load_dotenv()
def connectMongoQuestionsDB():
    mongoURI = os.getenv("MONGODB_URI")
    myclient = pymongo.MongoClient(mongoURI)
    return myclient

threadExecutor = ThreadPoolExecutor(1)
dictUsers = {}
application = Flask(__name__)
mongoClient = connectMongoQuestionsDB()

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
        collection = session[DB_NAME]["featureMapErrors"]
        collection.replace_one(
            {"id": id}, {"id": id, "error": errorMessage, "timestamp": datetime.now()})

# Mongo Global vars
DB_NAME = "slim-prediction"
COLLECTION_NAME = "JSONInfo"


def generateFeatureMapCreationTask(bucketName, content):
    '''
    Create as many threads as possible on this machine parallelizing the processing of an image into feature maps
    :param bucketName: S3 Bucket Name
    :param content: UUID coming from the Go Server
    :return:
    '''
    fileTuples = []

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucketName)
    lstS3Files = list(bucket.objects.filter(Prefix="img/" + content['UUID']))

    count = 100
    for idx in range(0, len(lstS3Files), count):
        fileTuples.append((idx, min(idx + count, len(lstS3Files))))

    with ThreadPoolExecutor() as executor:
        args = []
        for fileChunkIdx in fileTuples:
            args.append(
                [bucketName, lstS3Files[fileChunkIdx[0]: fileChunkIdx[1]], content['UUID']])
        results = executor.map(generateFeatureMap, args)


def generateFeatureMap(args):
    '''
    Processing method to extract features from images
    :param args: Args[0] = Bucket Name, Args[1] = Package (list of S3 Objects) divided from the complete list
    :return: True on Success, None otherwise
    '''
    # Gets the information in bytes
    indexer = Index()

    bucketName = args[0]
    package = args[1]
    id = args[2]

    failed = False
    while failed == False:
        try:
            s3Client = boto3.client('s3')
            failed = True
        except Exception as error:
            mongoReplace(id, "Failed to connect to aws.")
            time.sleep(0.5)
            pass

    for obj in package:
        try:
            awsImageBytes = obj.get()['Body'].read()  # try except here
        except Exception as error:
            mongoReplace(id, "Failed to get the Body of the s3 object.")
            print(error)
            break
        try:
            awsImageBytesFeatures = indexer.extract_single_feature(  # try except here
                awsImageBytes, True)
        except Exception as error:
            mongoReplace(id, "Failed to extract the single feature.")
            print(error)
            break
        fileName = obj.key.replace(
            "img/", "featuremap/").rsplit('.', 1)[0] + ".bin"
        try:
            s3Client.upload_fileobj(io.BytesIO(  # try except here
                awsImageBytesFeatures.tobytes()), bucketName, fileName)
        except Exception as error:
            mongoReplace(id, "Failed to upload feature map to aws.")
            print(error)
            break
    return True


@application.route("/get-result/<id>", methods=['GET'])
def getResult(id):
    '''
    Get the Result from an S3 upload
    :param id: uuid of current task
    :return: JSON of the current state
    '''
    if id not in dictUsers:
        return json.dumps({'id': id, 'result': 2, 'desc': 'id unknown'})
    if dictUsers[id].running():
        return json.dumps({'id': id, 'result': 1, 'desc': 'running'})
    elif dictUsers[id].cancelled():
        return json.dumps({'id': id, 'result': 3, 'desc': 'cancelled'})
    elif dictUsers[id].done():
        del (dictUsers[id])
        return json.dumps({'id': id, 'result': 0, 'desc': 'done'})
    else:
        return json.dumps({'id': id, 'result': -1, 'desc': 'not started yet'})


@application.route("/set-feature-maps", methods=['POST'])
def startFeatureMapMicroservice():
    content = request.json
    bucketName = os.getenv("S3_BUCKET")
    id = content['UUID']
    dictUsers[id] = threadExecutor.submit(
        generateFeatureMapCreationTask, bucketName, content)

    if not session:
        mongoReplace(id, 'Error getting the Feature Map MS DB.')

    return json.dumps({'id': id})


@ application.route("/", methods=['GET'])
def index():
    return 'Running'


if __name__ == '__main__':
    application.run(debug=True, threaded=True)
