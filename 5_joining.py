"""
This program will download transcoded files and join them to
single file.
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
import json # Handle javascript object notation data
import yaml # Reading yaml file in python
import math # Generate random numbers
import boto3 # Python module to interact with AWS.
import threading # Enable multitasking, would be use for s3 multipart upload
from pymongo import MongoClient #Mongo client to interact with mongodb
from kubernetes import client, config, watch #kubernetes job specific modules
from kubernetes.client import models as mymodel #kubernetes job specific modules
from boto3.s3.transfer import TransferConfig #S3 upload specific module

#Megabyte to byte conversion 1048576 bytes
MB = 1024 * 1024
s3 = boto3.resource('s3')

class TransferCallback:

    def __init__(self, target_size):
        self._target_size = target_size
        self._total_transferred = 0
        self._lock = threading.Lock()
        self.thread_info = {}

    def __call__(self, bytes_transferred):
        """
        The callback method that is called by the transfer manager.

        Display progress during file transfer and collect per-thread transfer
        data. This method can be called by multiple threads, so shared instance
        data is protected by a thread lock.
        """
        thread = threading.current_thread()
        with self._lock:
            self._total_transferred += bytes_transferred
            if thread.ident not in self.thread_info.keys():
                self.thread_info[thread.ident] = bytes_transferred
            else:
                self.thread_info[thread.ident] += bytes_transferred

            target = self._target_size * MB
            sys.stdout.write(
                f"\r{self._total_transferred} of {target} transferred "
                f"({(self._total_transferred / target) * 100:.2f}%).")
            sys.stdout.flush()

def download_with_default_configuration(bucket_name, object_key,
                                        download_file_path, file_size_mb):
    """
    Download a file from an Amazon S3 bucket to a local folder, using the
    default configuration.
    """
    transfer_callback = TransferCallback(file_size_mb)
    s3.Bucket(bucket_name).Object(object_key).download_file(
        download_file_path,
        Callback=transfer_callback)
    return transfer_callback.thread_info

def upload_with_chunksize_and_meta(local_file_path, bucket_name, object_key,
                                   file_size_mb, metadata=None):
    """
    Upload a file from a local folder to an Amazon S3 bucket, setting a
    multipart chunk size and adding metadata to the Amazon S3 object.

    The multipart chunk size controls the size of the chunks of data that are
    sent in the request. A smaller chunk size typically results in the transfer
    manager using more threads for the upload.

    The metadata is a set of key-value pairs that are stored with the object
    in Amazon S3.
    """
    transfer_callback = TransferCallback(file_size_mb)

    config = TransferConfig(multipart_chunksize= 2 * MB)
    extra_args = {'Metadata': metadata} if metadata else None
    s3.Bucket(bucket_name).upload_file(
        local_file_path,
        object_key,
        Config=config,
        ExtraArgs=extra_args,
        Callback=transfer_callback)
    return transfer_callback.thread_info

bucket_name = "psltranscoder"
file_size_mb = 1000


#DB Initialization
db_client = MongoClient("mongodb://db.bornincloudstreaming.com:27017/") #DB server address
db = db_client["CoreDB"] #Database name CoreDB
transcodeDb = db["transcodeDb"]
"""Database table name transcodeDb.
It is related to transcoding job status.
"""
frontEndDb = db["jobDetails"]
"""Database table name jobDetails.
It is related to job submission by fronend UI
"""
templateDb = db["templateDetails"]
"""
Database table which store profiles template.
"""
bitrateLadder = db["profileDetails"]
"""
Database table which store multiple profiles to be use for
transcoding.
"""
k8sDb = db["k8sDb"]
"""
Database table which store k8s job related metadata.
"""

# Variable Initialization
psls3Bucket = "/media" #local folder in the container
distributed = os.path.join(psls3Bucket,"intermediate","distributed") #Right hand side is nested folder in S3
splitPath = os.path.join(psls3Bucket,"intermediate","split") #Right hand side is nested folder in S3
outputDirectoryMultipart = os.path.join("intermediate","split") #Right hand side is nested folder in S3
splitPathMultipart = "intermediate/split"
output = os.path.join(psls3Bucket, "output") #Right hand side is folder in S3
outputSplit=os.path.join(psls3Bucket,"intermediate","splitCompress") #Right hand side is nested folder in S3
outputComplete = os.path.join(psls3Bucket, "output") #Right hand side is folder in S3
rejected = os.path.join(psls3Bucket,"intermediate","rejected") #Right hand side is nested folder in S3
tempFile = os.path.join(psls3Bucket, "temp", "out.txt") #Right hand side is a temp file under nested folder in S3
localPath = "/video" #local folder in the container
localPathDel = "/video" #local folder in the container
baseLocalPath ="/"
gopFactor = 2 # Group of pictures


#Function :- Join
def joinSplitFile(jobId,profileName,jsContentId,timetowait):
    frontEndDb.update_one({"jobId":jobId}, {"$set":{"join":"in progress"}})
#    profileName = myProfile.get(str(profileName))
    results = transcodeDb.find({"contentId":jsContentId})
    for result in results:
        outputSplitPath = result["outputSplitPath"]
        outputSinglePath = result["outputSinglePath"]
        fileName = result['revFileName']
        s = outputSinglePath
    outputSinglePathMultiPart = s.removeprefix('/media/')
    f_str = fileName
    fresult = f_str.split(".",1)[0]
    os.chdir(localPath)
    a = str(fresult) + "_" + profileName
    if os.path.exists(a):
        shutil.rmtree(a)
    os.mkdir(a)
    localContentId = os.path.join(localPath,a)
    os.chdir(localContentId)
    if os.path.exists(profileName):
        shutil.rmtree(profileName)
    os.mkdir(profileName)
    localprofileName = os.path.join(localContentId,profileName)
    os.chdir(localprofileName)
    fileDst = os.path.join(outputSinglePath,profileName)
    pathProfile = os.path.join(outputSplitPath,profileName)
    joinFiles = os.listdir(pathProfile)
    chunkfilename = "chunks.txt"
    with open(chunkfilename , 'w') as fout:
        for file in joinFiles:
            src=os.path.join(pathProfile, file)
            srcMultipart = src.removeprefix('/media/')
            download_file_path = os.path.join(localprofileName,file)
            object_key = srcMultipart
            download_with_default_configuration(bucket_name, object_key,download_file_path, file_size_mb)
#            shutil.move(src,localprofileName)
            fout.write(file + '\n')
    chunks = list()
    with open (chunkfilename) as fin:
        for line in fin:
            chunks.append(line.strip())
    splitchunkname = list()
    for name in chunks:
        x = name.replace(".mp4", "")
        splitchunkname.append(x)
#Convert string to int
    res = [eval(i) for i in splitchunkname]
    res.sort()
    with open("File.txt", 'w') as f:
        for result1 in res:
            name = str(result1) + ".mp4"
            f.write("file '%s'\n" %name)
    joinFile = os.path.join(localprofileName,"File.txt")
    final = a + ".mp4"
    finalFileName = os.path.join(outputSinglePath,profileName,final)
    finalFileNameMultipart = os.path.join(outputSinglePathMultiPart,profileName,final)
    txtfinalFileName = os.path.join(outputSinglePath,profileName,"File.txt")
    command = f"ffmpeg -y -f concat -safe 0 -i {joinFile} -c copy {final}"
    tempFile = os.path.join(psls3Bucket, "temp", "out.txt")
    with open(tempFile, 'w') as f:
        results = subprocess.Popen(command,stdout = f,stderr = f,shell=True)
    # waitToJoin = k8sDb.find({"contentId":jsContentId})
    # for value in waitToJoin:
    #     sleepForJoin = value["timeToSplit"]
#Sleep parameter need to tweak.
  #  time.sleep(int(sleepForJoin * 2 ))
    time.sleep(timetowait * 2)
    joinSleepTime = timetowait * 2
    k8sDb.update_one({"contentId":jsContentId}, {"$set":{"joinSleepTime(In Min)":joinSleepTime}})
    local_file_path = os.path.join(localprofileName,final)
    object_key = finalFileNameMultipart
    upload_with_chunksize_and_meta(local_file_path, bucket_name,object_key, file_size_mb)
    os.chdir(localPath)
    shutil.rmtree(localContentId)
    transcodeDb.update_one(
                            {
                                "contentId":jsContentId
                            },
                            {
                                "$set":{
                                    a:"Joining Complete"
                                    }
                            })
    return jsContentId


def psltoBeJoin(jobId,psljContentId,timetowait):
    frontEndDb.update_one({"jobId":jobId}, {"$set":{"join":"started"}})
    joinStart = datetime.datetime.now()
    transcodeDb.update_one(
                            {
                                "contentId":psljContentId
                            },
                            {
                                "$set":{
                                    "Joining Status":"Started",
                                    "Joining Start":joinStart
                                    }
                            })
    findSplitDuration = k8sDb.find({"contentId":psljContentId})
    for value in findSplitDuration:
        splitTimeInSec = value["splitTimeInSec"]
    results = transcodeDb.find({"contentId":psljContentId})
    for result in results:
        template = result['template']
    results = templateDb.find({"templatename": template})
    for result in results:
        profiles = result['profiles']
    for i in profiles:
        if i == profiles[-1]:
            status = joinSplitFile(jobId,i,psljContentId,timetowait)
            print(status)
        joinSplitFile(jobId,i,psljContentId,timetowait)
    now = datetime.datetime.now()
    transcodeDb.update_one(
                            {
                                "contentId":psljContentId
                            },
                            {
                                "$set":{
                                    "Joining End":now,
                                    "Joining Status":"Done",
                                    "Completion Time":now
                                }
                            })
    frontEndDb.update_one({"jobId":jobId}, {"$set":{"join":"completed"}})
    transcodeDb.update_one({"contentId":psljContentId}, {"$set":{"Packaging Require":"Yes"}})
    return psljContentId


##Program Start from here
flag = "No"
myContentId = "Null"
results = transcodeDb.find({'$and': [{"Joining Status": "Not Initiated"}, {"Compression Status": "Done"}]})
for result in results:
    myContentId = result["contentId"]
    jobId = result["jobId"]
    retryCount = result["retryCount"]
    flag = "Yes"
    t1 = result["Splitting Start"]
    t2 = result["Splitting End"]
    hhmmss = str(t2 - t1)
    [hours, minutes, seconds] = [int(float(x)) for x in hhmmss.split(':')]
    x = datetime.timedelta(hours=hours, minutes=minutes, seconds=seconds)
    timetowait = int(float(x.seconds))


if flag == "Yes" :

    try:
        #contentIdtoBeJoin = toBeJoin(contentIdToBeTranscode)
        pslcontentIdtoBeJoin = psltoBeJoin(jobId,myContentId,timetowait)
    except:
        if retryCount < 3:
            retryCount = retryCount + 1
            transcodeDb.update_one(
                                    {
                                        "contentId":myContentId
                                    },
                                    {
                                        "$set":{
                                            "Remarks":"Unknown error during Joining ,sent to rejoining",
                                            "Joining Status": "Not Initiated",
                                            "Error Stage":"Joining",
                                            "retryCount":retryCount
                                        }
                                    })

            frontEndDb.update_one(
                                    {
                                        "jobId":jobId
                                    },
                                    {
                                        "$set":{
                                            "retryCount":retryCount,
                                            "Error Stage":"Joining",
                                            "retryCount":retryCount
                                        }
                                    })
        else:
            frontEndDb.update_one(
                                    {
                                        "jobId":jobId
                                    },
                                    {
                                        "$set":{
                                            "status":"Failure , Retry Count 3 Exceeded",
                                            "Failed Content Id": myContentId,
                                            "Error Stage":"Joining"
                                        }
                                    })
            transcodeDb.update_one(
                                    {
                                        "contentId":myContentId
                                    },
                                    {
                                        "$set":{
                                            "Remarks":"Failure , Retry Count 3 Exceeded",
                                            "Error Stage":"Joining"
                                        }
                                    })
    else:
        transcodeDb.update_one({"contentId":pslcontentIdtoBeJoin}, {"$set":{"Packaging Require":"Yes"}})
