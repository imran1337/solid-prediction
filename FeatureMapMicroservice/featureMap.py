import pandas as pd
from tensorflow.keras.applications.vgg16 import VGG16, preprocess_input
from tensorflow.keras.models import Model
from tensorflow.keras.preprocessing import image
import tensorflow as tf
from PIL import Image
from tqdm import tqdm
import numpy
import os
import io
tf.compat.v1.logging.set_verbosity(tf.compat.v1.logging.ERROR)


class Index:

    sqlEngine = None

    # metaData = None

    # TODO: This needs to be changed when hosted on a cloud space
    indexBasePath = None

    def __init__(self):
        # self.metaData = MetaData()
        self.FE = FeatureExtractor()

    def extract_single_feature(self, imgPath, isBytes):
        return self.FE.get_single_feature(imgPath, isBytes)

    # Bulk extraction function
    def start_feature_extraction(self, analysisFolder, ifExists):
        jsonNew = []
        for obj in os.listdir(analysisFolder):
            if obj.endswith('png') or obj.endswith('jpg'):
                jsonNew.append(os.path.join(
                    analysisFolder, obj).replace('\\', '/'))

        image_data = pd.DataFrame()
        image_data['image_path'] = jsonNew
        image_data['features'] = self.FE.get_feature(image_data['image_path'])
        # Replace Vendor by vendor variable given
        image_data = image_data.dropna().reset_index(drop=True)
        image_data.to_sql('image_features', self.sqlEngine, if_exists=ifExists)
        return image_data


class FeatureExtractor:
    def __init__(self):
        # Use VGG-16 as the architecture and ImageNet for the weight
        base_model = VGG16(weights='imagenet')
        # Customize the model to return features from fully-connected layer
        self.model = Model(inputs=base_model.input,
                           outputs=base_model.get_layer('fc1').output)

    def extract(self, img):
        # Resize the image
        img = img.resize((224, 224))
        # Convert the image color space
        img = img.convert('RGB')
        # Reformat the image
        x = image.img_to_array(img)
        x = numpy.expand_dims(x, axis=0)
        x = preprocess_input(x)
        # Extract Features
        feature = self.model.predict(x)[0]
        return feature / numpy.linalg.norm(feature)

    def get_single_feature(self, image_data, isBytes=False):
        try:
            if isBytes:
                feature = self.extract(img=Image.open(io.BytesIO(image_data)))
            else:
                feature = self.extract(img=Image.open(image_data))
            return feature
        except:
            return numpy.array([])

    def get_feature(self, image_data: list):
        self.image_data = image_data
        # fe = FeatureExtractor()
        features = []
        for img_path in tqdm(self.image_data):  # Iterate through images
            # Extract Features
            try:
                feature = self.extract(img=Image.open(img_path))
                features.append(feature)
            except:
                features.append(None)
                continue
        return features
