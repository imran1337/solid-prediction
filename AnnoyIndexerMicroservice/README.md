# AnnoyIndexerMicroservice Setup Guide

## Setup Steps

1. Set up the required environment variables by executing the following command:

```bash
cp .env.sample .env
```

Update the values of the environment variables in the newly created `.env` file. Ensure that you replace `./solidmetaprediction-a9d846238df8.json` with the correct path to your Google Cloud Platform service account credentials.

2. Install project dependencies:

```bash
pip install -r requirements.txt
```

3. Run the Flask application with the following commands:

```bash
export FLASK_APP=application.py
export GOOGLE_APPLICATION_CREDENTIALS=./solidmetaprediction-a9d846238df8.json # Ask fellow developers for service key file.
export FLASK_DEBUG=1
flask run
```

The application will start running on the specified host and port, typically at `http://127.0.0.1:5000/`. You can access the API endpoints from this address.

## Additional Notes

- Ensure secure handling of your Google Cloud Platform credentials.
- For troubleshooting or additional information, refer to the project documentation or seek assistance from project contributors.