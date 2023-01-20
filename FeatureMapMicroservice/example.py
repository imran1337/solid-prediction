from flask import Flask, Response, render_template, stream_with_context, request
import urllib.parse
import json
from model import Question, getDB
import time
import uuid
import datetime
import threading

application = Flask(__name__)
dbConnection = getDB()

dictThreads={}
def backgroundTask(jsonObj):
    ret = Question.getQuestions(dbConnection, jsonObj['data'])
    print(ret)
    time.sleep(5)
    print("Completed Task")
    return ret

@application.route('/getQuestion/<id>', methods=['GET'])
def getQuestion(id):
    print(id)
    if id not in dictThreads:
        return Response(
            "Unknown ID",
            status=400,
        )

    if type(dictThreads[id][0]) != list:
        return json.dumps({'status': 'calculating'})
    elif type(dictThreads[id][0]) == list:
        return json.dumps(dictThreads[id][0])
    else:
        return Response(
            "Internal error",
            status=500,
        )

@application.route('/setQuestion', methods=['POST'])
def setQuestion():
    strJson = request.get_data()
    strJson = urllib.parse.unquote(strJson)
    jsonObj = json.loads(strJson)
    retUuid = uuid.uuid4()
    dictThreads[str(retUuid)] = [lambda: (backgroundTask(jsonObj)), 0]
    response_object = {
            "task_id": retUuid
    }
    return response_object

class QuestionGatherer(threading.Thread):

    do_run = False

    event_exit = None

    def __init__(self, args):
        threading.Thread.__init__(self, args=args)
        self.event_exit = args[0]

    def run(self):
        while True:
            for obj in list(dictThreads.keys()):
                if type(dictThreads[obj][0]) != list:
                    dictThreads[obj][0] = dictThreads[obj][0]()
                    dictThreads[obj][1] = datetime.datetime.now()
                print(obj)
                #if type(dictThreads[obj][0]) == list and (datetime.datetime.now() - dictThreads[obj][1]).seconds > 10:
                #    del(dictThreads[obj])



if __name__ == '__main__':
    global serverThread
    e = threading.Event()
    questionGatherThread = QuestionGatherer((e,))
    questionGatherThread.daemon = True
    questionGatherThread.start()
    application.run(debug=True, threaded=True, port=5001)