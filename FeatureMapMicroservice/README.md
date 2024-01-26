# FeatureMapMicroservice Setup Guide

## Setup Steps

1. Begin by setting up the necessary environment variables using the following command:

```bash
cp .env.sample .env
```

Update the values in the newly created `.env` file, replacing `./solidmetaprediction-a9d846238df8.json` with the correct path to your Google Cloud Platform service account credentials.

2. Install the project dependencies:

```bash
pip install -r requirements.txt
```

3. Start the Flask application with the following commands:

```bash
export FLASK_APP=application.py
export GOOGLE_APPLICATION_CREDENTIALS=./solidmetaprediction-a9d846238df8.json # Request the service key file from fellow developers.
export FLASK_DEBUG=1
flask run
```

The application will be running on the specified host and port, typically at `http://127.0.0.1:5000/`. Access the API endpoints from this address.

# FeatureMapMicroservice File Upload Guide

We employ a custom encryption method for file encryption and use an `.exe` file for decryption, which is designed for Windows. However, file upload is also possible on Mac or Linux, and we will detail the process later.

## Uploading an SMP file using Windows

1. Create a folder named `static`. Inside the static folder, create another folder called `upload`.
2. Place the `clidecrypt.exe` file within the `static` folder (request the exe file from a fellow developer).
3. Open the `testing.py` file.
4. Update the `filePath` variable with the absolute path of your SMP file.
5. Modify the `vendor` name in the `data` variable.
6. Run `python testing.py` to upload the file.

## Uploading an SMP file using Mac or Linux

1. Open the `testing2.py` file.
2. Modify the `filename` variable with the name of your zip file. Next, update the `vendor` name in the `data` variable.
3. Ensure the zip file is located in the root folder, i.e., under the `FeatureMapMicroservice` folder.
4. To add support for uploading a zip file, modify the code by following these steps:
    - Open `fileOperations.py` and add the provided function:
      ```python
      def copy_zip_file(file_path, fileId):
        static_folder = os.path.join(os.getcwd(), 'static')
        dest_path = os.path.join(static_folder, 'upload', fileId + '.zip')

        try:
            shutil.copy2(file_path, dest_path)
            print('File copy completed successfully.')
            return dest_path
        except Exception as e:
            print(f'File copy failed with error: {e}')
            return ''
      ```
    - In `application.py`, locate `fileOperations.decrypt_file` within the `process_file` function, and replace it with `copy_zip_file`.
5. Run `python testing.py` to upload the file.

## Additional Notes

- Ensure the secure handling of your Google Cloud Platform credentials.
- For troubleshooting or additional information, refer to the project documentation or seek assistance from project contributors.