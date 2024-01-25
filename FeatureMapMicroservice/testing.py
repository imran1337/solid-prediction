import requests
import json
import os

file_name = "VW536_760_Touareg_PA_MY24_MP1_SMALL.zip"
filePath = os.path.join(os.path.dirname(__file__), file_name)

data = {'user': 'mhaenssgen@unevis.de', 'vendor': 'Touareg_PA', 'version': 1}
with open(filePath, 'rb') as ffile:
    try:
        with requests.post('http://127.0.0.1:5000/uploadFile', files={'file': ffile}, data={'json_data': json.dumps(data)}) as r:
            r.raise_for_status()
            print('File upload successful.')
    except requests.exceptions.RequestException as e:
        print(e)
