import subprocess
import os
import zipfile
import pymongo
import json
import uuid
import hashlib
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import boto3

def decrypt_file(file_path, fileId):
    # Construct the command
    destPath = 'static/upload/' + fileId
    command = ['static/clidecrypt.exe', '-f', file_path, '-dst', destPath]

    # Execute the command
    try:
        subprocess.run(command, check=True)
        print('File decryption completed successfully.')
    except Exception as e:
        print(f'File decryption failed with error: {e}')
        return ''

    return os.path.join(file_path).replace('smp', 'zip')

def get_hash(filename):
    try:
        with open(filename, 'r') as file:
            file_info = file.read()
    except IOError as e:
        print(e)
        return None

    try:
        result = json.loads(file_info)
    except json.JSONDecodeError as e:
        print(e)
        return None

    # Get params objects
    params = result.get('parameters', {})
    sorted_params = dict(sorted(params.items()))  # Sort params dictionary by keys
    json_str = json.dumps(sorted_params, sort_keys=True)
    # Convert to hash
    params_hash = hashlib.sha256(json_str.encode('utf-8')).digest()
    params_hash_hex = params_hash.hex()

    return params_hash_hex

# Function to unzip the file
def unzip_file(file_path):
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        # Extract all files to a desired location
        extract_path = file_path.replace('.zip', '')  # Replace with your desired location
        if not os.path.exists(extract_path):
            os.makedirs(extract_path)

        zip_ref.extractall(extract_path)

    return extract_path

def getUniqueFileId():
    return str(uuid.uuid4())

def addJsonToMongo(json_file_path, additionalInfo, client, mongodb_database, mongodb_collection):
    # Load JSON data from file
    print('blubb')
    print(json_file_path)
    with open(json_file_path, 'r') as file:
        print(json_file_path)
        print(file)
        json_data = json.load(file)
        #print(json_data)

    print('blubberdubber')
    #print(json_data)
    # Connect to MongoDB
    db = client[mongodb_database]
    collection = db[mongodb_collection]

    dictPresets = {}
    # Insert each record into MongoDB
    for record in json_data:
        print(record)
        newRecord = {}
        # This will stop if no preset file is present in the record
        hashVal = get_hash(os.path.join(os.path.dirname(json_file_path), 'preset', record['preset_file_name']))
        dictPresets[record['preset_file_name']] = hashVal + '.preset'
        newRecord['category'] = record['category']
        newRecord['file_name'] = record['file_name']
        newRecord['tri_ratio_median'] = record['tri_ratio_median']
        newRecord['rating_0_10'] = record['rating_0_10']
        newRecord['conn_avg'] = record['conn_avg']
        newRecord['rating_raw'] = record['rating_raw']
        newRecord['min_offset'] = record['min_offset']
        newRecord['offset_ratio'] = record['offset_ratio']
        newRecord['surface_area'] = record['surface_area']
        newRecord['vertexcount'] = record['vertexcount']
        newRecord['bbox_diagonal'] = record['bbox_diagonal']
        newRecord['edge_len_avg'] = record['edge_len_avg']
        newRecord['max_offset'] = record['max_offset']
        newRecord['border_ratio'] = record['border_ratio']
        newRecord['vtx_devn_ratio'] = record['vtx_devn_ratio']
        newRecord['tri_ratio_avg'] = record['tri_ratio_avg']
        newRecord['curvature_avg'] = record['curvature_avg']
        newRecord['polyisland_count'] = record['polyisland_count']
        newRecord['facecount'] = record['facecount']
        newRecord['pointcount'] = record['pointcount']
        newRecord['material_count'] = record['material_count']
        newRecord['material_names'] = record['material_names']
        newRecord['image_file_names'] = [additionalInfo['uuid'] + obj for obj in record['image_file_names']]
        newRecord['preset_file_name'] = hashVal + '.preset'
        newRecord['universal_uuid'] = additionalInfo['uuid']
        newRecord['parent_package_name'] = additionalInfo['parent_package_name']
        newRecord['version'] = additionalInfo['version']
        newRecord['user'] = additionalInfo['user']
        newRecord['vendor'] = additionalInfo['vendor']
        try:
            collection.insert_one(newRecord)
        except Exception as e:
            print(e)
            return {}
        #Testing
        #break

    print(f"Records added to MongoDB collection '{mongodb_collection}' successfully.")
    return dictPresets

def uploadFileType(destZipPath, fileType, bucket, s3_client, fileId, dictPresets={}):
    fileTuples = []
    arrFiles = os.listdir(os.path.join(destZipPath, fileType))

    count = 100
    for idx in range(0, len(arrFiles), count):
        fileTuples.append((idx, min(idx + count, len(arrFiles))))

    with ThreadPoolExecutor() as executor:
        args = []
        for fileChunkIdx in fileTuples:
            args.append([[os.path.join(destZipPath, fileType, obj)
                         for obj in arrFiles[fileChunkIdx[0]: fileChunkIdx[1]]],
                         bucket, s3_client, fileId, dictPresets])
        results = executor.map(putInS3, args)

    #for obj in os.listdir(os.path.join(destZipPath, fileType)):
    #    location, error = putInS3(os.path.join(destZipPath, fileType, obj), bucket, s3_client, fileId, dictPresets)
    #    if error is not None:
    #        print(f"Error uploading file: {error}")
    #    else:
    #        print(f"File uploaded successfully. Location: {location}")

        # Testing
        #return None

def doesPresetExist(s3_client, bucket, name, dictPresets):
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix="preset/")
    content = response.get("Contents", [])
    if os.path.basename(name) in dictPresets:
        for obj in content:
            if dictPresets[os.path.basename(name)] == obj['Key']:
                return True
    return False

def putInS3(args):
    #file_path, bucket, s3_client, uuid, dictPresets={}
    arrFiles = list(args[0])
    bucket = args[1]
    s3_client = args[2]
    uuid = args[3]
    dictPresets = dict(args[4])

    for file_path in arrFiles:
        if not Path(file_path).exists():
            continue
            #raise FileNotFoundError(f"File not found: {file_path}")

        folder = ""
        file_ext = os.path.splitext(file_path)[1]
        if file_ext == ".png":
            folder = "img/"
            file_name = os.path.basename(file_path)
            path = folder + uuid + file_name
        elif file_ext == ".preset":
            if doesPresetExist(s3_client, bucket, file_path, dictPresets):
                continue
                #return dictPresets[os.path.basename(file_path)], None
            folder = "preset/"
            path = folder + dictPresets[os.path.basename(file_path)]
        else:
            continue
            #return '', None

        with open(file_path, "rb") as ffile:
            uploader = boto3.Session().client("s3").upload_fileobj
            uploader(ffile, bucket, path)

        #return path, None