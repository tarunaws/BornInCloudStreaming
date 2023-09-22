"""
This program will generate the content id and change the status of
transcoding job from "not initiated" to "started".
This program also use to pause the transcoding job , till the time underline
resources are not available.
"""
#Import Modules
import os #Interact with Operating system
import shutil #Use for file copy/paste
import datetime #working with date command
import subprocess #Process to be run in background
import time #Identify time
import uuid #Generate unique id
import sys #Better control over input and output
import random # Generate random numbers
from pymongo import MongoClient #Mongo client to interact with mongodb


#DB Initialization
db_client = MongoClient("mongodb://db.bornincloudstreaming.com:27017/") #DB server address
db = db_client["CoreDB"] #Database name CoreDB
transcodeDb = db["transcodeDb"]
"""Database table name transcodeDb.
It is related to transcoding job status
"""
frontEndDb = db["frontenddbs"]
"""Database table name frontenddbs.
It is related to job submission by fronend UI
"""

#Variable Initialization
psls3Bucket = "/media" #local folder in the container
inputFolder = os.path.join(psls3Bucket, "input") #Right hand side is folder in S3
distributed = os.path.join(psls3Bucket,"intermediate","distributed") #Right hand side is nested folder in S3
archive = os.path.join(psls3Bucket, "archive") #Right hand side is folder in S3
jobId = "value" #Dummy value

#Content id Generation
def cid():
    """
    This function will generate unique id for every
    submitted video file , that unique id would be use to
    refer video files, everywhere in the program.
    """
    now = int(time.time()) * random.randint(2, 10)
    """
    Convert epoch time from float to integer then multiply
    with any random number
    """
    myContentId = str(now) #convert integer to string
    return myContentId

#Program start from here, if transcoding status not initiated , then start transcoding.
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
