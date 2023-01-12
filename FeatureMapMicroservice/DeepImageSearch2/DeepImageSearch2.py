import numpy
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    BLOB
)

import os
import json
import io
import pandas as pd
import matplotlib.pyplot as plt
from PIL import Image
from tqdm import tqdm
import numpy as np
from annoy import AnnoyIndex
from tqdm import tqdm
from tensorflow.keras.preprocessing import image
from tensorflow.keras.applications.vgg16 import VGG16, preprocess_input
from tensorflow.keras.models import Model
import tensorflow as tf
tf.compat.v1.logging.set_verbosity(tf.compat.v1.logging.ERROR)

class LoadData:
    """Loading the data from Single/Multiple Folders or form CSV file"""
    def __init__(self):
        pass
    def from_folder(self,folder_list:list): # Enter the Single Folder Path/List of the Folders
        self.folder_list = folder_list
        image_path = []
        for folder in self.folder_list:
            for path in os.listdir(folder):
                image_path.append(os.path.join(folder,path))
        return image_path # Returning list of images
    def from_csv(self,csv_file_path:str,images_column_name:str): # CSV File path with Images path Columns Name
        self.csv_file_path = csv_file_path
        self.images_column_name = images_column_name
        return pd.read_csv(self.csv_file_path)[self.images_column_name].to_list() # Returning list of images

class FeatureExtractor:
    def __init__(self):
        # Use VGG-16 as the architecture and ImageNet for the weight
        base_model = VGG16(weights='imagenet')
        # Customize the model to return features from fully-connected layer
        self.model = Model(inputs=base_model.input, outputs=base_model.get_layer('fc1').output)
    def extract(self, img):
        # Resize the image
        img = img.resize((224, 224))
        # Convert the image color space
        img = img.convert('RGB')
        # Reformat the image
        x = image.img_to_array(img)
        x = np.expand_dims(x, axis=0)
        x = preprocess_input(x)
        # Extract Features
        feature = self.model.predict(x)[0]
        return feature / np.linalg.norm(feature)

    def get_single_feature(self, image_data, isBytes=False):
        try:
            if isBytes:
                feature = self.extract(img=Image.open(io.BytesIO(image_data)))
            else:
                feature = self.extract(img=Image.open(image_data))
            return feature
        except:
            return numpy.array([])

    def get_feature(self,image_data:list):
        self.image_data = image_data 
        #fe = FeatureExtractor()
        features = []
        for img_path in tqdm(self.image_data): # Iterate through images 
            # Extract Features
            try:
                feature = self.extract(img=Image.open(img_path))
                features.append(feature)
            except:
                features.append(None)
                continue
        return features

class Index:

    sqlEngine = None

    metaData = None

    #TODO: This needs to be changed when hosted on a cloud space
    indexBasePath = None

    def __init__(self):
        self.metaData = MetaData()
        self.FE = FeatureExtractor()

    def extract_single_feature(self, imgPath, isBytes):
        return self.FE.get_single_feature(imgPath, isBytes)

    # Bulk extraction function
    def start_feature_extraction(self, analysisFolder, ifExists):
        jsonNew = []
        for obj in os.listdir(analysisFolder):
            if obj.endswith('png') or obj.endswith('jpg'):
                jsonNew.append(os.path.join(analysisFolder, obj).replace('\\', '/'))

        image_data = pd.DataFrame()
        image_data['image_path'] = jsonNew
        image_data['features']  = self.FE.get_feature(image_data['image_path'])
        # Replace Vendor by vendor variable given
        image_data = image_data.dropna().reset_index(drop=True)
        image_data.to_sql('image_features', self.sqlEngine, if_exists=ifExists)

        #image_data = pd.read_sql('image_features', self.sqlEngine)
        #print(image_data.head().to_string())
        #print("Image Meta Information Saved: [meta-data-files/image_data_features.pkl]")
        #print(image_data.head().to_string())
        return image_data

        # image_data.to_pickle(config.image_data_with_features_pkl)
        # print(image_data.to_string())

    def start_indexing(self, image_data, indexerPath, vendor):
        indexerPath = os.path.join(indexerPath, 'annoy_indexer')
        if not os.path.exists(indexerPath):
            os.makedirs(indexerPath)

        f = len(image_data['features'][0]) # Length of item vector that will be indexed
        t = AnnoyIndex(f, 'euclidean')
        for i,v in tqdm(zip(image_data.index, image_data['features'])):
            t.add_item(i, v)
            #print(t, i, v)
        t.build(100) # 100 trees
        print(os.path.join(indexerPath, vendor + '_fvecs.ann'))
        t.save(os.path.join(indexerPath, vendor + '_fvecs.ann'))

class SearchImage:

    sqlEngine = None

    vendor = ''

    indexVendorPath = ''

    imageData = []

    features = 0

    def __init__(self, parts, indexerPath, vendor):

        self.vendor = vendor

        self.indexVendorPath = indexerPath

        #self.imageData = [obj['features'] for obj in parts]
        lvi = 0
        for obj in parts:
            if lvi == 0:
                self.features = len(np.fromstring(obj.features, dtype=np.float32))
                lvi += 1
            self.imageData.append(obj.imagefilename)

        #self.image_data = pd.read_sql('image_features', self.sqlEngine)
        #self.f = len(self.image_data['features'][0])

    def search_by_vector(self, v, n:int):
        self.v = v # Feature Vector
        self.n = n # number of output 

        u = AnnoyIndex(self.features, 'euclidean')
        u.load(self.indexVendorPath) # super fast, will just mmap the file
        index_list = u.get_nns_by_vector(self.v, self.n) # will find the 10 nearest neighbors
        arr = numpy.array(self.imageData)
        #print(dict(zip(index_list, self.imageData)))
        return list(arr[index_list])

    def get_query_vector(self, image_path:str):
        self.image_path = image_path
        if not os.path.exists(image_path):
            return None
        img = Image.open(self.image_path)
        fe = FeatureExtractor()
        query_vector = fe.extract(img)
        return query_vector

    def get_similar_images(self, image_path:str, number_of_images:int):
        self.image_path = image_path
        self.number_of_images = number_of_images
        query_vector = self.get_query_vector(self.image_path)
        arrImgs = []
        if query_vector is not None:
            arrImgs = self.search_by_vector(query_vector, self.number_of_images)
        return arrImgs