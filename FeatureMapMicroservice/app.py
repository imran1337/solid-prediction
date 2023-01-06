import sys
from flask import Flask, request, jsonify
from DeepImageSearch2.DeepImageSearch2 import Index, SearchImage
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os

app = Flask(__name__)

load_dotenv()

@app.route("/FeatureMapMicroservice", methods=['POST'])
def FeatureMapMicroservice():
    #content = request.json
    #print(content)
    imgPath = "testImages/_000.019.906.J__TM__001_---_KiSi_G2-3_15bis36kg_ISOFIT._ppH.fbx.backView.png"
    s3 = boto3.resource('s3')
    bucket = os.getenv("S3_BUCKET") 
    awsImage = s3.Object(bucket, "img/a0413b18-48da-4759-9ff0-bdbc08f03d9f_000.019.906.J__TM__001_---_KiSi_G2-3_15bis36kg_ISOFIT._ppH.fbx.backView.png")
    # Gets the information in bytes. I can download the picture, maybe working with bytes is faster?
    awsImageBytes = awsImage.get()['Body'].read()
    indexer = Index()
    awsImageBytesFeatures = indexer.extract_single_feature(awsImageBytes, True)
    imgFeature = indexer.extract_single_feature(imgPath, False)
    normalImageFeatures = imgFeature.tobytes()
    bytesImageFeatures = awsImageBytesFeatures.tobytes()
    if normalImageFeatures == bytesImageFeatures:
        print("Images Features are equal")
    imagefeaturepath = imgPath
    
    return "Received"
