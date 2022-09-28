from pprint import pprint
from flask import Flask, render_template
import numpy as np
import pickle,json
from datetime import *
try:
    import Db
    import conf
except Exception as e:
    print(e," in fe.py")
try:
    from . import Db
    from . import conf
except Exception as e:
    print(e," in fe.py")

db = Db.Db()
_op = "<span class='_high'>"
_cl = "</span>"

def getBest(data):
    # print("*******",data)
    response = None
    for row in data:
        if row["grain"]=="minute":
            response = row
            break
    if response is None:
        for row in data:
            if row["grain"]=="day":
                response = row
                break
    if response is None:
        for row in data:
            if row["grain"]=="month":
                response = row
                break
    if response is None:
        for row in data:
            if row["grain"]=="hour":
                response = row
                break
    if response is None:
        for row in data:
            if row["grain"]=="year":
                response = row
                break
    if response is None:
        response = data[0]
    return response


app = Flask(__name__)
@app.route('/new')
def new():
    # return conf.targetUrl
    response = db.getNewData(conf.targetDomain)
    # print(response)
    tuples = []
    for row in response:
        tupleObj = {}
        values = list(json.loads(row["diffDate"]).values())
        ## take TODO priority--day,moth year
        if len(values)>0:
            value = getBest(values)

            ts = value["val"]
            if (ts is None or ts==""):
                if value["from"] != None and value["from"]!="":
                    ts += "from: "+value["from"]
                if value["to"] != None and value["to"]!="":
                    ts += "  -  to:"+value["to"]
            if value["grain"]!="":
                ts+= (" ("+value["grain"]+") ")
            tupleObj["text"] = row["newVal"][:value["start"]] +_op+value["text"]+_cl+row["newVal"][value["end"]:]
            tupleObj["ts"] = ts
            tupleObj["url"] = row["url"]
            tupleObj["link"] = row["url"]+"#:~:text="+value["text"]
            tuples.append(tupleObj)
    # print(tuples)
    data ={
        "tuples":tuples,
        "page": "new"
    }
    # print(data)
    return render_template('new.html',data = data)
    # return render_template('new.html', response)


if __name__=="__main__":
    app.run(host="0.0.0.0", port=7777, use_reloader = True, debug = True)
