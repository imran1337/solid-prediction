import sys
from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route("/FeatureMapMicroservice", methods=['POST'])
def FeatureMapMicroservice():
    content = request.json
    print(content)
    return "Received"
