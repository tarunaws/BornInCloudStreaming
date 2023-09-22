#Import Modules
import os,shutil,datetime,subprocess,time,uuid
from pymongo import MongoClient
import sys

#DB Initialization
db_client = MongoClient("mongodb://bornincloudstreaming.com:27777/")
db = db_client["CoreDB"]
frontEndDb = db["frontenddbs"]

# def cid():
# #    data = str(uuid.uuid4())[:6]
#     now = int(time.time()) * 2
# #    myContentId = "BIC"+str(data) + str(now)[:6]
# #    myContentId = str(now)[:4]
#     myContentId = str(now)
#     return myContentId

jobId = str(uuid.uuid4())
filename = "file1.mp4"
bucket = "psltranscoder"
filepath = "/input_video/"




jobDetail = {
    'jobId' : jobId,
    'filename' : filename,
    'bucket' : bucket,
    'filepath' : filepath ,
    'retryCount' : 0,
    'analyse' : "not started"
    }
frontEndDb.insert_one(jobDetail)
