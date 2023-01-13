import sys
from flask import Flask, request, current_app, send_file
from DeepImageSearch2.DeepImageSearch2 import Index, SearchImage
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os
import io
import numpy
import pymongo
import pandas
import json
import zipfile
import uuid

app = Flask(__name__)

load_dotenv()

def CleanUpFiles(filepath):
        os.remove(filepath)

@app.route("/feature-map-microservice", methods=['POST'])
def FeatureMapMicroservice():
    content = request.json
    print(content)
    s3 = boto3.resource('s3')
    s3Client = boto3.client('s3')
    bucketName = os.getenv("S3_BUCKET")

    awsImageBytesList = []

    # Gets the information in bytes. I can download the picture, maybe working with bytes is faster?
    indexer = Index()

    bucket = s3.Bucket(bucketName)
    objs = bucket.objects.filter(Prefix="img/" + content['UUID'])

    #idx=0
    for obj in objs:
        awsImageBytes = obj.get()['Body'].read()
        print("Aws Image Bytes to Feature Start:")
        awsImageBytesFeatures = indexer.extract_single_feature(awsImageBytes, True)
        fileName = obj.key.replace("img/", "featuremap/" ).rsplit('.', 1)[0] + ".bin"
        s3Client.upload_fileobj(io.BytesIO(awsImageBytesFeatures.tobytes()), bucketName, fileName)

        # For testing purposes
        #if idx >= 5:
         #   break
        #idx += 1
    
    return "Received"
@app.route("/annoy-indexer", methods=['GET'])
def AnnoyIndexer():
    currentPath = current_app.root_path
    test = os.listdir(currentPath)

    for item in test:
        if item.endswith(".zip"):
            os.remove(os.path.join(currentPath, item))

    generatedUUID = str(uuid.uuid4())   
    
    indexer = Index()

    vendor = "Volkswagen"

    # Init AWS
    s3 = boto3.resource('s3')
    bucketName = os.getenv("S3_BUCKET")
    bucket = s3.Bucket(bucketName)
    objs = bucket.objects.all()

    # Init MongoDB
    mongoURI = os.getenv("MONGODB_URI")
    myclient = pymongo.MongoClient(mongoURI)
    mydb = myclient["test"]
    mycol = mydb["JSONInfo"]
    temp = []

    # Check MongoDB to see if the JSON files have a PSF File

    mypsfquery = {"psf_file_name": {"$exists": True}, "vendor":vendor}

    
    # TODO Check to see what happens when nothing is found

    mydoc = mycol.find(mypsfquery, {"image_file_names": 1})

    for x in mydoc:
        temp += x["image_file_names"]

    imagesDict = {"image_file_names": temp}

    # Get the feature maps of the specific images
    featureFiles = ["featuremap/" + obj.rsplit('.', 1)[0] + ".bin" for obj in temp]
    arrayParts = []
    idx = 0
    for featureFile in featureFiles:
         # Check if the object exists or if we need to error handle it
        try:
            awsImage = s3.Object(bucketName, featureFile) 
            #print(awsImage)
        except ClientError:
            print(featureFile)
            continue
        # Append images to an array. And buffer them. 
        awsImageBytes = awsImage.get()['Body'].read()

        arrNumpy = numpy.frombuffer(awsImageBytes, dtype=numpy.float32)
        arrayParts.append(arrNumpy)
        if idx == 0:
            imagesDict['length'] = len(arrNumpy)
            idx += 1

    # Create a dataframe and create the indexer
    df = pandas.DataFrame()
    df['features'] = arrayParts

    downloadLocation = os.path.dirname(__file__)
    if getattr(sys, 'frozen', False):
        downloadLocation = os.path.dirname(sys.executable)
    downloadLocation += '/DownloadFiles'
    downloadTotalPath = os.path.join(currentPath, downloadLocation, generatedUUID)
    indexer.start_indexing(df, downloadTotalPath, vendor)
    indexerPath = os.path.join(downloadTotalPath, vendor + '_fvecs.ann')

    # Create JSON File
    json_object = json.dumps(imagesDict, indent=4)
    jsonPath = os.path.join(downloadTotalPath, "info.json")

    with open(jsonPath, "w") as outfile:
        outfile.write(json_object)

    # Download Both 
    # ZIp files
    zipLocation = os.path.join(downloadTotalPath, generatedUUID + ".zip")
    with zipfile.ZipFile(zipLocation, 'w', zipfile.ZIP_DEFLATED) as zipObj:
        zipObj.write(jsonPath, os.path.basename(jsonPath))
        zipObj.write(indexerPath, os.path.basename(indexerPath))

    # Cleanup files
    os.remove(jsonPath)
    os.remove(indexerPath)
    # TODO: Build a function that cleans up the whole directory after a day

    return send_file(zipLocation, as_attachment=True)

@app.route("/find-matching-part", methods=['POST'])
def FindMatchingPart():
    content = request.json
    #print(content)
    mongoURI = os.getenv("MONGODB_URI")
    myclient = pymongo.MongoClient(mongoURI)
    mydb = myclient["test"]
    mycol = mydb["JSONInfo"]

    objectIdMongo = []

    for x in content["data"]:
        mydoc = mycol.distinct("_id", {"image_file_names":x})
        if mydoc in objectIdMongo:
            continue
        else:
            objectIdMongo.append(mydoc)
    listToReturn = []
    for x in objectIdMongo:
        fullMongoObjects = mycol.find_one({"_id":x[0]})
        listToReturn.append(fullMongoObjects)

    return listToReturn



if __name__ == '__main__':
    app.run(debug=True, threaded=True, port=5001)