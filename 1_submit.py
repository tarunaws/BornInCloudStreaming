"""
Below program will capture the detail of newly added file , which
has been submitted for transcoding
bucket_name = "bornincloud-transcoder"
"""


#Import Modules
import os #Interact with Operating system
import shutil #Use for file copy/paste
import datetime #working with date command
import subprocess #Process to be run in background
import time #Identify time
import uuid #Generate unique id
import sys #Better control over input and output
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
s3Bucket = "/media" #local folder in the container
inputFolder = os.path.join(s3Bucket, "input") #Right hand side is folder in S3
distributed = os.path.join(s3Bucket,"intermediate","distributed") #Right hand side is nested folder in S3
archive = os.path.join(s3Bucket, "archive") #Right hand side is folder in S3
jobId = "value" #Dummy value

#file name revision
def reviseName(each):
    """
    During transcoding operation,sometime same filename gets submitted more than two times.
    This can break the transcoding job, because same name would be already available in database,
    so it is necessary to revise the file name if it has submitted 2nd time.
    """
    tmp = "null"
    #Identify the already available file name in DB.
    results = transcodeDb.find({"fileName":each})
    for result in results:
        tmp1 = result["revFileName"]
        tmp = result["fileName"]
    if tmp == each:
        f_str = tmp1
        #Extract left hand side of a string.i.e a from a.mp4
        fresult = f_str.split(".",1)[0]
        #Extract right hand side of a string.i.e mp4 from a.mp4
        fresult1 = f_str.split(".",2)[1]
        x = datetime.datetime.now() #Example: datetime.datetime(2023, 9, 5, 12, 1, 20, 510559)
        newDateTime = x.strftime("%d") + x.strftime("%b") + x.strftime("%Y") + "T" + x.strftime("%H") + ":" + x.strftime("%M") + ":" + x.strftime("%S")
         """
         Example : 05Sep2023T12:02:24
         %d: Extract date
         %b: Extract Month
         %y: Extract year
         "T and : ": Just a seperator
         %H: Extract time in hours
         %M: Extract time in minutes
         %S: Extract time in seconds
         """
        tmp =  fresult + "_Revise_" + newDateTime + "." + fresult1
        return tmp.replace(" ","")#Remove extra space from final string
    else:
        return each.replace(" ","")#Remove extra space from final string

#Program start from here,Check for not started job from frontend DB table.
frfilename = "null1"
results = frontEndDb.find({"analyse": "not started"})
for result in results:
    frfilename = result['filename']
    basePath = result["filepath"]
    frBucketName = result["bucket"]
    jobId = result["jobId"]
    retryCount = result["retryCount"]
if frfilename != "null1":
    frontEndDb.update_one({"jobId":jobId}, {"$set":{"analyse":"inprocess"}})
    copyToTranscoderStartTime = datetime.datetime.now()
    frontEndDb.update_one({"jobId":jobId}, {"$set":{"status":"inprocess"}})
    #Remove last slash from base path /media/asd/fgg/ to be converted as /media/asd/fgg
    frBasePath = basePath[1:-1]
    download_file_path = os.path.join(inputFolder,frfilename)
    object_key = frBasePath + "/" + frfilename
    each = frfilename
    revFileName1 = reviseName(each)
    #Add basic video file detail into Transcode DB Table
    listDbMetadata = {
        'jobId' : jobId,
        'fileName' : each,
        'revFileName':revFileName1,
        'Transcoding Status' : "Not Initiated",
        'Compression Status' : "Not Initiated",
        'Splitting Status' : "Not Initiated",
        'object_key' : object_key,
        'bucketName' : frBucketName,
        'retryCount' : retryCount
        }
    transcodeDb.insert_one(listDbMetadata)
