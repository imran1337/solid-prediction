import sys
from flask import Flask, request, jsonify
from DeepImageSearch2.DeepImageSearch2 import Index, SearchImage
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os
import io
import numpy
import pymongo

app = Flask(__name__)

load_dotenv()

@app.route("/FeatureMapMicroservice", methods=['POST'])
def FeatureMapMicroservice():
    content = request.json
    print(content)
    imgPath = "testImages/_000.019.906.J__TM__001_---_KiSi_G2-3_15bis36kg_ISOFIT._ppH.fbx.backView.png"
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
@app.route("/AnnoyIndexer", methods=['POST'])
def AnnoyIndexer():

    indexer = Index()

    # Init AWS
    s3 = boto3.resource('s3')
    bucketName = os.getenv("S3_BUCKET")
    bucket = s3.Bucket(bucketName)
    objs = bucket.objects.all()

    # Init MongoDB
    myclient = pymongo.MongoClient("mongodb://mongo:FBbvQCE8X6T4wwqSpMvB@containers-us-west-107.railway.app:7638")
    mydb = myclient["test"]
    mycol = mydb["JSONInfo"]
    myquery = {"vendor": "Volkswagen"}

    mydoc = mycol.find(myquery)
    temp = []

    for x in mydoc:
        temp += x['image_file_names']
    featureFiles = ["featuremap/" + obj.rsplit('.', 1)[0] + ".bin" for obj in temp]
    arrayParts = []
    for featureFile in featureFiles:

        try:
            awsImage = s3.Object(bucketName, featureFile) 
            print(awsImage)
        except ClientError:
            print(featureFile)
            continue
        
        # Check if the object exists or if we need to error handle it

        awsImageBytes = awsImage.get()['Body'].read()

        # These lines need to be checked. Dont know if they work
        arrayParts.append(numpy.frombuffer(awsImageBytes, dtype=numpy.float32))
        #print(arrayParts)
        df = pandas.DataFrame()
        df['features'] = arrParts
        indexer.start_indexing(df, indexerPath, vendor)
        
        # Add a dataframe
        # Add the features to the dataframe and start the annoy Index



    #for obj in objs:
    #    awsImageBytes = obj.get()['Body'].read()
    
    
    #print(awsImageBytes)

    return "Received"