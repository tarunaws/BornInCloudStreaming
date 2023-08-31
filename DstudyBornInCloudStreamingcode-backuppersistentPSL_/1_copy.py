#Import Modules
import os,shutil,datetime,subprocess,time,uuid
from pymongo import MongoClient
import sys

#DB Initialization
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

#file name revision
def reviseName(each):
    tmp = "null"
    results = transcodeDb.find({"fileName":each})
    for result in results:
        tmp1 = result["revFileName"]
        tmp = result["fileName"]
    if tmp == each:
        f_str = tmp1
        fresult = f_str.split(".",1)[0]
        fresult1 = f_str.split(".",2)[1]
        x = datetime.datetime.now()
        newDateTime = x.strftime("%d") + x.strftime("%b") + x.strftime("%Y") + "T" + x.strftime("%H") + ":" + x.strftime("%M") + ":" + x.strftime("%S")
        tmp =  fresult + "_Revise_" + newDateTime + "." + fresult1
        return tmp.replace(" ","")
    else:
        return each.replace(" ","")

#Program start from here
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
    frBasePath = basePath[1:-1]
    download_file_path = os.path.join(inputFolder,frfilename)
    object_key = frBasePath + "/" + frfilename
    each = frfilename
    revFileName1 = reviseName(each)
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
