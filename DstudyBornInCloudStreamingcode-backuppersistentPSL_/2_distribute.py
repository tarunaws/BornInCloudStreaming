#Import Modules
import os,shutil,datetime,subprocess,time,uuid,random
from pymongo import MongoClient
import sys

#DB Initialization k8s
db_client = MongoClient("mongodb://db.bornincloudstreaming.com:27017/")
db = db_client["CoreDB"]
transcodeDb = db["transcodeDb"]
frontEndDb = db["frontenddbs"]

#Variable Initialization
s3Bucket = "/media"
inputFolder = os.path.join(s3Bucket, "input")
distributed = os.path.join(s3Bucket,"intermediate","distributed")
archive = os.path.join(s3Bucket, "archive")
jobId = "value"


#Content id Generation
def cid():
    now = int(time.time()) * random.randint(2, 10)
    myContentId = str(now)
    return myContentId

#Program start from here
transcodingStatus = "null"
compressionStatus = "null"
results = transcodeDb.find({"Transcoding Status":"Not Initiated"})
for result in results:
    transcodingStatus = result["Transcoding Status"]
    revFileName = result["revFileName"]
    compressionStatus = result["Compression Status"]
    jobId = result["jobId"]
if  transcodingStatus == "Not Initiated" :
    myContentId = cid()
    transcodeDb.update_one(
                            {
                            "revFileName":revFileName
                            },
                            {
                            "$set":{
                                    "Transcoding Status":"Copy in Process",
                                    "contentId":myContentId,
                                    "Transcoding Status":"Waiting for resource"
                                    }
                                    })
    frontEndDb.update_one(
                            {
                                "jobId":jobId
                            },
                            {
                                "$set":{
                                    "analyse":"inprocess",
                                    "status":"inprocess"
                                }
                            })
