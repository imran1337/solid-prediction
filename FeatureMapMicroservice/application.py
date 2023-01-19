from flask import Flask, request
import boto3
import os
import io
import pymongo
from featureMap import Index


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


@application.route("/", methods=['GET'])
def HelloWorld():
    return "Hello World"


@application.route("/feature-map-microservice", methods=['POST'])
def FeatureMapMicroservice():
    content = request.json
    print(content)
    s3 = boto3.resource('s3')
    s3Client = boto3.client('s3')
    bucketName = os.getenv("S3_BUCKET")

    # Gets the information in bytes. I can download the picture, maybe working with bytes is faster?
    indexer = Index()

    bucket = s3.Bucket(bucketName)
    objs = bucket.objects.filter(Prefix="img/" + content['UUID'])

    for obj in objs:
        awsImageBytes = obj.get()['Body'].read()
        print("Aws Image Bytes to Feature Start:")
        awsImageBytesFeatures = indexer.extract_single_feature(
            awsImageBytes, True)
        fileName = obj.key.replace(
            "img/", "featuremap/").rsplit('.', 1)[0] + ".bin"
        s3Client.upload_fileobj(io.BytesIO(
            awsImageBytesFeatures.tobytes()), bucketName, fileName)

    return "Received"


if __name__ == '__main__':
    application.run(debug=True, threaded=True)
