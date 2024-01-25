import shutil

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
import fileOperations
from google.cloud import storage

from dotenv import load_dotenv
load_dotenv(dotenv_path='../dot.env')
def connectMongoQuestionsDB():
    mongoURI = os.getenv("MONGODB_URI")
    myclient = pymongo.MongoClient(mongoURI)
    return myclient

threadExecutor = ThreadPoolExecutor(1)
dictUsers = {}
application = Flask(__name__)
mongoClient = connectMongoQuestionsDB()

# Set up Google Cloud Storage client
gcs_client = storage.Client()

# Your Google Cloud Storage bucket name
gcs_bucket_name = os.getenv("GOOGLE_CLOUD_BUCKET_ID")
gcs_bucket = gcs_client.bucket(gcs_bucket_name)

processing_complete = False

# Define the signal handler for when processing is complete
def processing_completed(signum, frame):
    global processing_complete
    processing_complete = True


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
    # session = _checkDBSession(mongoClient)
    # if session:
    #     collection = session[DB_NAME]["featureMapErrors"]
    #     collection.replace_one(
    #         {"id": id}, {"id": id, "error": errorMessage, "timestamp": datetime.now()})
    print(f"Found error {id} - {errorMessage}")

# Mongo Global vars
DB_NAME = "slim-prediction"
COLLECTION_NAME = "JSONInfo"#"Testing"


def generateFeatureMapCreationTask(bucketName, id):
    '''
    Create as many threads as possible on this machine parallelizing the processing of an image into feature maps
    :param bucketName: GCS Bucket Name
    :param id: UUID coming from the Go Server
    :return:
    '''
    try:
        fileTuples = []

        client = storage.Client()
        bucket = client.bucket(bucketName)

        print("Called")
        
        try:
            lstGCSObjects = list(bucket.list_blobs(prefix="img/" + id)) 
        except Exception as err:
            print(f"error in lstGCSObjects process")


        count = 100
        for idx in range(0, len(lstGCSObjects), count):
            fileTuples.append((idx, min(idx + count, len(lstGCSObjects))))

        with ThreadPoolExecutor() as executor:
            args = []
            for fileChunkIdx in fileTuples:
                chunk = lstGCSObjects[fileChunkIdx[0]: fileChunkIdx[1]]
                blob_list = [blob for blob in chunk] 
                args.append([bucketName, blob_list, id])

            results = executor.map(generateFeatureMap, args)

            print(f'===results=== {results}')

    except Exception as err:
        print(f"Error in generateFeatureMapCreationTask process: {err}")



def generateFeatureMap(args):
    '''
    Processing method to extract features from images
    :param args: Args[0] = Bucket Name, Args[1] = Package (list of GCS Blob objects) divided from the complete list
    :return: True on Success, False otherwise
    '''
    # Gets the information in bytes
    indexer = Index()

    bucketName = args[0]
    package = args[1]
    id = args[2]

    failed = False
    while not failed:
        try:
            # Set up Google Cloud Storage client
            gcs_client = storage.Client()

            failed = True
        except Exception as error:
            mongoReplace(id, "Failed to connect to GCS.")
            time.sleep(0.5)
            pass

    for obj in package:
        try:
            gcsImageBytes = obj.download_as_bytes()
        except Exception as error:
            mongoReplace(id, "Failed to get the Body of the GCS object.")
            print(error)
            return False  # Return False on failure

        try:
            gcsImageBytesFeatures = indexer.extract_single_feature(gcsImageBytes, True)
        except Exception as error:
            mongoReplace(id, "Failed to extract the single feature.")
            print(error)
            return False  # Return False on failure

        fileName = obj.name.replace("img/", "featuremap/").rsplit('.', 1)[0] + ".bin"
        try:
            bucket = gcs_client.get_bucket(bucketName)
            blob = bucket.blob(fileName)
            blob.upload_from_file(io.BytesIO(gcsImageBytesFeatures.tobytes()), content_type="application/octet-stream")
        except Exception as error:
            mongoReplace(id, "Failed to upload feature map to GCS.")
            print(error)
            return False  # Return False on failure

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

# TODO: Add additional functions that either only upload the images or the presets or the DB entries, if something needs
# to updated
def process_file(filename, additionalInfo, fileId):
    # Perform your long-running file processing task here
    # destPath = fileOperations.decrypt_file(filename, fileId)
    destPath = fileOperations.copy_zip_file(filename, fileId)
    additionalInfo = json.loads(additionalInfo)
    additionalInfo['uuid'] = fileId
    print('-2')
    if destPath:
        destZipPath = fileOperations.unzip_file(destPath)
        print('-1')
        if destZipPath:
            jsonFiles = [obj for obj in os.listdir(destZipPath) if obj.endswith('.json')]
            jsonPath = os.path.join(destZipPath, jsonFiles[0])
            print(jsonPath)
            additionalInfo['parent_package_name'] = os.path.basename(destZipPath)
            print('0')
            dictPresets = fileOperations.addJsonToMongo(jsonPath, additionalInfo, mongoClient, DB_NAME, COLLECTION_NAME)
            print('1')
            # Usage example
            bucket = os.environ['GOOGLE_CLOUD_BUCKET_ID']
            # session = boto3.Session()
            # s3_client = session.client("s3")


            # GCS Bucket and Blob setup
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket)

            print('2')
            fileOperations.uploadFileType(destZipPath, 'img', bucket, storage_client, fileId)
            print('3')
            fileOperations.uploadFileType(destZipPath, 'preset', bucket, storage_client, fileId, dictPresets)
            print('4')
            generateFeatureMapCreationTask(gcs_bucket_name, fileId)
            print('5')
        else:
            print('Path not valid')
    else:
        print('Path not valid')

    shutil.rmtree('static/upload/' + fileId)

@application.route('/uploadFile', methods=['POST'])
def upload_file():
    file = request.files['file']
    data = request.form.get('json_data')

    filename = file.filename
    fileId = fileOperations.getUniqueFileId()
    destPath = 'static/upload/' + fileId
    if not os.path.exists(destPath):
        os.makedirs(destPath)

    # Save the uploaded file to a desired location
    filePath = os.path.join(destPath, filename)
    file.save(filePath)

    dictUsers[fileId] = threadExecutor.submit(process_file, filePath, data, fileId)
    # session = _checkDBSession(mongoClient)
    # if not session:
    #     mongoReplace(fileId, 'Error getting the Feature Map MS DB.')

    return json.dumps({'id': fileId})

    # Start the file processing task using ThreadPoolExecutor
    #with ThreadPoolExecutor() as executor:
    #    executor.submit(process_file, filePath, data)

    #return 'File uploaded successfully.'

# Create an endpoint to check the processing status
@application.route('/status', methods=['GET'])
def check_status():
    global processing_complete
    return 'Processing complete' if processing_complete else 'Processing in progress'


@ application.route("/", methods=['GET'])
def index():
    return 'Running'

if __name__ == '__main__':
    application.run(debug=True, threaded=True)
