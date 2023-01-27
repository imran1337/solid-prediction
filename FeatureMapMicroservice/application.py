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

threadExecutor = ThreadPoolExecutor(1)
dictUsers = {}
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
            args.append([bucketName, lstS3Files[fileChunkIdx[0]: fileChunkIdx[1]]])
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

    failed = False
    while failed == False:
        try:
            s3Client = boto3.client('s3')
            failed = True
        except Exception as error:
            print(error)
            time.sleep(0.5)
            pass

    for obj in package:
        awsImageBytes = obj.get()['Body'].read()
        awsImageBytesFeatures = indexer.extract_single_feature(awsImageBytes, True)
        fileName = obj.key.replace("img/", "featuremap/").rsplit('.', 1)[0] + ".bin"
        s3Client.upload_fileobj(io.BytesIO(awsImageBytesFeatures.tobytes()), bucketName, fileName)

    return True

@application.route("/get-result/<id>", methods=['GET'])
def getResult(id):
    '''
    Get the Result from an S3 upload
    :param id: uuid of current task
    :return: JSON of the current state
    '''
    if id not in dictUsers:
        return json.dumps({'id': id, 'result' : 2, 'desc': 'id unknown'})
    if dictUsers[id].running():
        return json.dumps({'id': id, 'result' : 1, 'desc': 'running'})
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
    id = str(uuid.uuid4())
    dictUsers[id] = threadExecutor.submit(generateFeatureMapCreationTask, bucketName, content)
    return json.dumps({'id': id})

@application.route("/", methods=['GET'])
def index():
    return 'Running'

if __name__ == '__main__':
    application.run(debug=True, threaded=True)
