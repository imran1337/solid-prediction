import requests
import json

filePath = r"D:\Professional\Unevis\VW\Cars\VWTouareg_2024_MP1\VW536_760_Touareg_PA_MY24_MP1.smp"
#r"D:\Professional\Unevis\VW\Cars\133_20220715_VW270_EU_K1_Polo_MY23_MP1\VW270_EU_K1_Polo_MY23_MP1_categorized_20230330.smp"

data = {'user': 'mhaenssgen@unevis.de', 'vendor': 'Volkswagen', 'version': 1}
with open(filePath, 'rb') as ffile:
    try:
        with requests.post('http://127.0.0.1:5000/uploadFile', files={'file': ffile}, data={'json_data': json.dumps(data)}) as r:
            r.raise_for_status()
            print('File upload successful.')
    except requests.exceptions.RequestException as e:
        print(e)


